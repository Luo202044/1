#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import sys
import random
from asyncio import Lock
from playwright.async_api import async_playwright

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

# 基础配置
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
BATCH_SIZE = config.get("batch_size", 200)
MAX_RETRY_ON_CLOSED = config.get("max_retry_on_closed", 3)

os.makedirs("data", exist_ok=True)

# 输出文件路径
if START_CID is not None and END_CID is not None:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "")
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

# 未完成标志文件
SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

# 全局状态
global_total = 0
global_completed = 0
global_lock = Lock()
write_buffer = []
buffer_lock = Lock()
start_time = None
stop_event = asyncio.Event()

# ---------- 代理和UA ----------
PROXY_LIST = []   # 填写代理 ["http://user:pass@ip:port"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ---------- 资源监控 ----------
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

# ---------- 批量写入 ----------
async def flush_buffer():
    global write_buffer
    if not write_buffer:
        return
    async with buffer_lock:
        if write_buffer:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write("".join(write_buffer))
            write_buffer.clear()

async def add_to_buffer(line):
    global write_buffer
    async with buffer_lock:
        write_buffer.append(line)
        if len(write_buffer) >= BATCH_SIZE:
            await flush_buffer()

# ---------- 页面提取 ----------
async def fetch_page_data(page, cid):
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    try:
        await asyncio.wait_for(
            page.goto(url, timeout=WAIT_TIMEOUT * 1000, wait_until="domcontentloaded"),
            timeout=30
        )
        body_text = await page.text_content("body") or ""
        if len(body_text.strip()) < 50:
            return "无", "无"
        try:
            await asyncio.wait_for(
                page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT * 1000),
                timeout=5
            )
        except:
            pass
        # 班级名称
        class_name = None
        elem = await page.query_selector("p.courseName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                class_name = text
        if not class_name:
            title = await page.title()
            if "|" in title and "Join the class" not in title:
                parts = title.split("|")
                if len(parts) > 1:
                    class_name = parts[-1].strip()
        class_str = class_name if class_name else "无"
        # 学校名称
        school_name = None
        elem = await page.query_selector("p.schoolName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                school_name = text
        school_str = school_name if school_name else "无"
        return school_str, class_str
    except asyncio.TimeoutError:
        raise Exception("操作超时")
    except Exception as e:
        raise e

async def process_cid_with_retry(page, cid, retry=0):
    try:
        return await fetch_page_data(page, cid)
    except Exception as e:
        if retry < RETRY_TIMES and ("timeout" in str(e).lower() or "net::" in str(e).lower()):
            delay = RETRY_DELAY * (2 ** retry)
            await asyncio.sleep(delay)
            return await process_cid_with_retry(page, cid, retry + 1)
        else:
            raise e

# ---------- Worker（带页面关闭重试） ----------
async def worker(browser, cid_list, worker_id):
    global global_completed, start_time
    context = None
    page = None
    closed_retry_count = {}

    async def ensure_page():
        nonlocal context, page
        if page and not page.is_closed():
            try:
                await page.close()
            except:
                pass
        if context:
            try:
                await context.close()
            except:
                pass
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            proxy={"server": random.choice(PROXY_LIST)} if PROXY_LIST else None,
            ignore_https_errors=True,
            java_script_enabled=True
        )
        page = await context.new_page()
        try:
            await page.evaluate("1+1")
        except Exception as e:
            raise Exception(f"新建 page 不可用: {e}")

    try:
        await ensure_page()
        for idx, cid in enumerate(cid_list):
            if stop_event.is_set():
                remaining = cid_list[idx:]
                if remaining:
                    with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                        for c in remaining:
                            f.write(f"{c}\n")
                break

            retry_cnt = closed_retry_count.get(cid, 0)
            if retry_cnt >= MAX_RETRY_ON_CLOSED:
                async with global_lock:
                    global_completed += 1
                print(f"[W{worker_id}] 放弃 {cid}: 页面关闭重试已达上限 {MAX_RETRY_ON_CLOSED} 次", flush=True)
                continue

            try:
                school_str, class_str = await process_cid_with_retry(page, cid)
                async with global_lock:
                    global_completed += 1
                    cur = global_completed

                is_valid = not (class_str == "无" and school_str == "无")
                if is_valid:
                    elapsed = time.time() - start_time
                    ratio = cur / global_total
                    remain_str = format_time((elapsed / ratio) - elapsed) if ratio > 0 else "未知"
                    mem_str = get_system_memory_str()
                    cpu_str = get_cpu_percent_str()
                    print(f"[W{worker_id}] ({cur}/{global_total}) 内存:{mem_str} cpu:{cpu_str}% 剩余:{remain_str} {cid} | {school_str} | {class_str}", flush=True)
                    await add_to_buffer(f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school_str} {class_str}\n")

                if cid in closed_retry_count:
                    del closed_retry_count[cid]

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
                    print(f"[W{worker_id}] 页面关闭 (cid={cid}, 重试 {closed_retry_count[cid]}/{MAX_RETRY_ON_CLOSED})，正在重建...", flush=True)
                    try:
                        await ensure_page()
                    except Exception as rebuild_err:
                        print(f"[W{worker_id}] 重建页面失败: {rebuild_err}", flush=True)
                        if "browser" in str(rebuild_err).lower():
                            break
                    continue
                else:
                    async with global_lock:
                        global_completed += 1
                    error_line = error_msg.split("\n")[0][:100]
                    print(f"[W{worker_id}] 错误 {cid}: {error_line}", flush=True)

            await asyncio.sleep(SLEEP_BETWEEN)

    finally:
        if page and not page.is_closed():
            await page.close()
        if context:
            await context.close()

# ---------- 主函数 ----------
async def main_async():
    global start_time, global_total
    start_time = time.time()

    # 确定扫描列表
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
    print(f"超时限制: {TIMEOUT_HOURS} 小时 ({TIMEOUT_SECONDS} 秒)")
    print(f"页面关闭最大重试: {MAX_RETRY_ON_CLOSED}")
    print(f"结果保存至: {OUTPUT_FILE}")
    print("控制台仅输出有效班级和简洁错误信息。\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
                "--disable-setuid-sandbox", "--disable-accelerated-2d-canvas",
                "--disable-background-networking", "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows", "--disable-breakpad",
                "--disable-client-side-phishing-detection", "--disable-default-apps",
                "--disable-extensions", "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                "--disable-hang-monitor", "--disable-ipc-flooding-protection",
                "--disable-popup-blocking", "--disable-prompt-on-repost",
                "--disable-renderer-backgrounding", "--disable-sync",
                "--disable-software-rasterizer", "--metrics-recording-only",
                "--no-first-run", "--safebrowsing-disable-auto-update",
                "--disable-logging", "--silent", "--js-flags=--max-old-space-size=128"
            ]
        )

        # 分配 CID 给 workers
        worker_count = min(MAX_CONCURRENT, len(cid_list))
        chunk_size = (len(cid_list) + worker_count - 1) // worker_count
        chunks = [cid_list[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count)]
        tasks = [asyncio.create_task(worker(browser, chunk, i)) for i, chunk in enumerate(chunks) if chunk]

        async def set_stop():
            await asyncio.sleep(TIMEOUT_SECONDS)
            stop_event.set()
            print(f"\n⚠️ 已达到运行时间上限（{TIMEOUT_HOURS}小时），停止接收新班级，等待现有任务完成...", flush=True)

        timeout_task = asyncio.create_task(set_stop())
        await asyncio.gather(*tasks, return_exceptions=True)
        timeout_task.cancel()
        await flush_buffer()
        await browser.close()

    elapsed_total = time.time() - start_time

    if stop_event.is_set():
        with open(UNFINISHED_FLAG, "w") as f:
            f.write(f"Timeout at {time.ctime()}\n")
            f.write(f"Processed {global_completed}/{global_total}\n")
        print(f"\n⚠️ 扫描未完成，已生成 {UNFINISHED_FLAG}", flush=True)

        # 合并 worker 未完成列表
        unfinished_cids = set()
        for i in range(worker_count):
            wfile = f"unfinished_worker_{i}.txt"
            if os.path.exists(wfile):
                with open(wfile, "r") as f:
                    for line in f:
                        if line.strip():
                            unfinished_cids.add(int(line.strip()))
                os.remove(wfile)
        if unfinished_cids:
            with open(f"unfinished_cids_{SHARD_IDX}.txt", "w") as f:
                for cid in sorted(unfinished_cids):
                    f.write(f"{cid}\n")
            print(f"记录了 {len(unfinished_cids)} 个未完成的 CID", flush=True)
    else:
        if os.path.exists(UNFINISHED_FLAG):
            os.remove(UNFINISHED_FLAG)
        print(f"\n✅ 扫描完成！总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}")

    print(f"最终处理班级数: {global_completed}/{global_total}", flush=True)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
