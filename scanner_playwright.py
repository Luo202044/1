#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import sys
import random
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutureTimeoutError
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ========== 强制无缓冲输出 ==========
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ========== 配置加载 ==========
if not os.path.exists("config.json"):
    print("错误: config.json 不存在", flush=True)
    sys.exit(1)

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

if not config.get("should_scan", True):
    print("should_scan 为 false，跳过扫描", flush=True)
    sys.exit(0)

START_CID = config.get("start_cid")
END_CID = config.get("end_cid")
CID_LIST_FILE = config.get("cid_list_file")
MAX_CONCURRENT = config.get("max_concurrent", 40)
WAIT_TIMEOUT = config.get("wait_timeout", 10)
RENDER_WAIT = config.get("render_wait", 0.5)
SLEEP_BETWEEN = config.get("sleep_between", 0.05)
RETRY_TIMES = config.get("retry_times", 1)
RETRY_DELAY = config.get("retry_delay", 0.5)
TIMEOUT_HOURS = config.get("timeout_hours", 5.5)
TIMEOUT_SECONDS = TIMEOUT_HOURS * 3600
FORCE_EXIT_WAIT = config.get("force_exit_wait", 300)
BATCH_SIZE = config.get("batch_size", 200)
MAX_RETRY_ON_CLOSED = config.get("max_retry_on_closed", 3)
WORKER_PROCESS_TIMEOUT = config.get("worker_process_timeout", 600)  # 每个Worker进程最大存活时间（秒）

os.makedirs("data", exist_ok=True)

if START_CID is not None and END_CID is not None:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "")
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

global_total = 0
global_completed = 0
global_lock = asyncio.Lock()
write_buffer = []
buffer_lock = asyncio.Lock()
start_time = None
stop_event = asyncio.Event()

PROXY_LIST = []
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

try:
    import psutil
    PSUTIL_AVAILABLE = True
    TOTAL_MEM_GB = psutil.virtual_memory().total / (1024 ** 3)
except ImportError:
    PSUTIL_AVAILABLE = False

def get_system_memory_str():
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        mem = psutil.virtual_memory()
        used_gb = mem.used / (1024 ** 3)
        return f"{used_gb:.2f}GB/{TOTAL_MEM_GB:.1f}GB"
    except:
        return "?GB/?GB"

def get_cpu_percent_str():
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        cpu = psutil.cpu_percent(interval=0)
        return f"{cpu:.1f}"
    except:
        return "?"

def format_time(seconds):
    if seconds < 0:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

async def flush_buffer():
    global write_buffer
    if not write_buffer:
        return
    async with buffer_lock:
        if write_buffer:
            try:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write("".join(write_buffer))
                write_buffer.clear()
            except Exception as e:
                print(f"写入错误: {e}", flush=True)

async def add_to_buffer(line):
    global write_buffer
    async with buffer_lock:
        write_buffer.append(line)
        if len(write_buffer) >= BATCH_SIZE:
            await flush_buffer()

# ---------- 独立进程运行的 Worker 函数 ----------
def run_worker_process(worker_id, cid_list, output_file, config_dict, proxy_list, user_agents):
    async def worker_async():
        WAIT_TIMEOUT = config_dict.get("wait_timeout", 10)
        RENDER_WAIT = config_dict.get("render_wait", 0.5)
        SLEEP_BETWEEN = config_dict.get("sleep_between", 0.05)
        RETRY_TIMES = config_dict.get("retry_times", 1)
        RETRY_DELAY = config_dict.get("retry_delay", 0.5)
        MAX_RETRY_ON_CLOSED = config_dict.get("max_retry_on_closed", 3)
        BATCH_SIZE = config_dict.get("batch_size", 200)
        
        completed_cids = set()
        worker_out_file = f"data/worker_{worker_id}_temp.txt"
        write_buffer = []

        async def worker_flush():
            nonlocal write_buffer
            if write_buffer:
                with open(worker_out_file, "a", encoding="utf-8") as f:
                    f.write("".join(write_buffer))
                write_buffer.clear()

        async def worker_add(line):
            nonlocal write_buffer
            write_buffer.append(line)
            if len(write_buffer) >= BATCH_SIZE:
                await worker_flush()

        async def fetch_page_data(page, cid):
            url = f"https://www.eeo.cn/s/a/?cid={cid}"
            print(f"[W{worker_id}] goto start {cid}", flush=True)
            try:
                await asyncio.wait_for(
                    page.goto(url, timeout=WAIT_TIMEOUT*1000, wait_until="domcontentloaded"),
                    timeout=WAIT_TIMEOUT + 5
                )
                print(f"[W{worker_id}] goto end {cid}", flush=True)
                
                body_text = await asyncio.wait_for(page.text_content("body") or "", timeout=WAIT_TIMEOUT)
                if len(body_text.strip()) < 50:
                    return "无", "无"
                
                try:
                    await asyncio.wait_for(
                        page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT*1000),
                        timeout=5
                    )
                except:
                    pass

                class_name = "无"
                school_name = "无"

                try:
                    elem = await asyncio.wait_for(page.query_selector("p.courseName"), timeout=WAIT_TIMEOUT)
                    if elem:
                        text = (await asyncio.wait_for(elem.inner_text(), timeout=WAIT_TIMEOUT)).strip()
                        if text and len(text) >= 2:
                            class_name = text
                except:
                    pass

                try:
                    elem = await asyncio.wait_for(page.query_selector("p.schoolName"), timeout=WAIT_TIMEOUT)
                    if elem:
                        text = (await asyncio.wait_for(elem.inner_text(), timeout=WAIT_TIMEOUT)).strip()
                        if text and len(text) >= 2:
                            school_name = text
                except:
                    pass

                if class_name == "无":
                    try:
                        title = await asyncio.wait_for(page.title(), timeout=WAIT_TIMEOUT)
                        if "|" in title and "Join the class" not in title:
                            parts = title.split("|")
                            if len(parts) > 1:
                                class_name = parts[-1].strip()
                    except:
                        pass

                return school_name, class_name
            except asyncio.TimeoutError:
                raise Exception("操作超时")
            except PlaywrightTimeoutError:
                raise Exception("Playwright超时")
            except Exception as e:
                raise e

        async def process_cid_with_retry(page, cid, retry=0):
            try:
                return await fetch_page_data(page, cid)
            except Exception as e:
                if retry < RETRY_TIMES and ("timeout" in str(e).lower() or "net::" in str(e).lower()):
                    delay = RETRY_DELAY * (2 ** retry)
                    await asyncio.sleep(delay)
                    return await process_cid_with_retry(page, cid, retry+1)
                else:
                    raise e

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
                    "--disable-setuid-sandbox", "--disable-accelerated-2d-canvas"
                ]
            )
            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                proxy={"server": random.choice(proxy_list)} if proxy_list else None,
                ignore_https_errors=True,
                java_script_enabled=True
            )
            page = await context.new_page()

            i = 0
            closed_retry_count = {}

            while i < len(cid_list):
                cid = cid_list[i]
                retry_cnt = closed_retry_count.get(cid, 0)
                if retry_cnt >= MAX_RETRY_ON_CLOSED:
                    with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                        f.write(f"{cid}\n")
                    i += 1
                    continue

                try:
                    school_str, class_str = await process_cid_with_retry(page, cid)
                    completed_cids.add(cid)
                    is_valid = not (class_str == "无" and school_str == "无")
                    if is_valid:
                        line = f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school_str} {class_str}\n"
                        await worker_add(line)
                    if cid in closed_retry_count:
                        del closed_retry_count[cid]
                    i += 1
                except Exception as e:
                    error_msg = str(e)
                    is_closed = any(phrase in error_msg for phrase in [
                        "Target page, context or browser has been closed",
                        "context has been closed",
                        "page has been closed",
                        "browser has been closed"
                    ])
                    if is_closed:
                        closed_retry_count[cid] = retry_cnt + 1
                        try:
                            await page.close()
                            await context.close()
                        except:
                            pass
                        try:
                            context = await browser.new_context(
                                user_agent=random.choice(user_agents),
                                proxy={"server": random.choice(proxy_list)} if proxy_list else None,
                                ignore_https_errors=True,
                                java_script_enabled=True
                            )
                            page = await context.new_page()
                        except:
                            await asyncio.sleep(1)
                    else:
                        with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                            f.write(f"{cid}\n")
                        i += 1
                await asyncio.sleep(SLEEP_BETWEEN)

            await worker_flush()
            await browser.close()

    asyncio.run(worker_async())
    return worker_id

# ---------- 主函数 ----------
async def main_async():
    global start_time, global_total
    start_time = time.time()

    if CID_LIST_FILE:
        with open(CID_LIST_FILE, "r") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
        global_total = len(cid_list)
        print(f"列表模式: 从 {CID_LIST_FILE} 读取 {global_total} 个班级")
    else:
        if START_CID is None or END_CID is None:
            print("错误: 必须指定 start_cid/end_cid 或 cid_list_file", flush=True)
            sys.exit(1)
        cid_list = list(range(START_CID, END_CID + 1))
        global_total = len(cid_list)
        print(f"区间模式: {START_CID} ~ {END_CID} (共 {global_total} 个)")

    print(f"Worker 并发数: {MAX_CONCURRENT}")
    print(f"批量写入大小: {BATCH_SIZE}")
    print(f"软超时限制: {TIMEOUT_HOURS} 小时 ({TIMEOUT_SECONDS} 秒)")
    print(f"强制退出等待: {FORCE_EXIT_WAIT} 秒")
    print(f"Worker进程超时: {WORKER_PROCESS_TIMEOUT} 秒")
    print(f"结果保存至: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")

    config_dict = {
        "wait_timeout": WAIT_TIMEOUT,
        "render_wait": RENDER_WAIT,
        "sleep_between": SLEEP_BETWEEN,
        "retry_times": RETRY_TIMES,
        "retry_delay": RETRY_DELAY,
        "max_retry_on_closed": MAX_RETRY_ON_CLOSED,
        "batch_size": BATCH_SIZE,
    }

    worker_count = min(MAX_CONCURRENT, len(cid_list))
    chunk_size = (len(cid_list) + worker_count - 1) // worker_count
    chunks = [cid_list[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count)]

    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        loop = asyncio.get_running_loop()
        futures = [loop.run_in_executor(None, run_worker_process, i, chunks[i], OUTPUT_FILE, config_dict, PROXY_LIST, USER_AGENTS) for i in range(worker_count)]

        try:
            await asyncio.wait_for(asyncio.gather(*futures), timeout=TIMEOUT_SECONDS + FORCE_EXIT_WAIT)
        except asyncio.TimeoutError:
            print(f"主进程超时，强制取消Worker", flush=True)
            for f in futures:
                f.cancel()

    all_valid = []
    for i in range(worker_count):
        temp_file = f"data/worker_{i}_temp.txt"
        if os.path.exists(temp_file):
            with open(temp_file, "r") as f:
                all_valid.extend(f.readlines())
            os.remove(temp_file)

    seen = set()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for line in all_valid:
            cid = line.split()[0]
            if cid not in seen:
                seen.add(cid)
                f.write(line)

    unfinished_cids = set()
    for i in range(worker_count):
        ufile = f"unfinished_worker_{i}.txt"
        if os.path.exists(ufile):
            with open(ufile, "r") as f:
                for line in f:
                    if line.strip():
                        unfinished_cids.add(int(line.strip()))
            os.remove(ufile)
    if unfinished_cids:
        with open(f"unfinished_cids_{SHARD_IDX}.txt", "w") as f:
            for cid in sorted(unfinished_cids):
                f.write(f"{cid}\n")
        print(f"记录了 {len(unfinished_cids)} 个未完成的 CID", flush=True)
    else:
        print("所有CID已成功处理", flush=True)

    elapsed_total = time.time() - start_time
    print(f"扫描结束，总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    mp.freeze_support()
    main()
