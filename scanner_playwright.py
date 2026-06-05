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
WORKER_STUCK_SECONDS = config.get("worker_stuck_seconds", 300)  # Worker卡死判定时间，默认5分钟

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
global_lock = Lock()
write_buffer = []
buffer_lock = Lock()
start_time = None
stop_event = asyncio.Event()

# Worker 状态跟踪
worker_last_progress = {}      # worker_id -> 上次完成数量或时间戳
worker_completed_count = {}    # worker_id -> 已处理数量（累计）
worker_assigned_cids = {}      # worker_id -> 分配的所有CID列表
worker_current_task = {}       # worker_id -> asyncio.Task 对象

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

async def fetch_page_data(page, cid, timeout_sec):
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    try:
        await asyncio.wait_for(
            page.goto(url, timeout=WAIT_TIMEOUT * 1000, wait_until="domcontentloaded"),
            timeout=timeout_sec
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
    cid_timeout = WAIT_TIMEOUT + (RETRY_TIMES * RETRY_DELAY) + 10
    try:
        return await asyncio.wait_for(fetch_page_data(page, cid, cid_timeout), timeout=cid_timeout)
    except asyncio.TimeoutError:
        raise Exception("CID处理超时")
    except Exception as e:
        if retry < RETRY_TIMES and ("timeout" in str(e).lower() or "net::" in str(e).lower()):
            delay = RETRY_DELAY * (2 ** retry)
            await asyncio.sleep(delay)
            return await process_cid_with_retry(page, cid, retry + 1)
        else:
            raise e

async def worker(browser, cid_list, worker_id):
    global global_completed, start_time, worker_last_progress, worker_completed_count
    context = None
    page = None
    closed_retry_count = {}
    assigned_cids = set(cid_list)
    completed_cids = set()
    # 记录该worker的初始进度
    worker_completed_count[worker_id] = 0
    worker_last_progress[worker_id] = time.time()

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
        i = 0
        while i < len(cid_list):
            if stop_event.is_set():
                remaining = [c for c in cid_list[i:] if c not in completed_cids]
                if remaining:
                    with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                        for c in remaining:
                            f.write(f"{c}\n")
                break

            cid = cid_list[i]
            retry_cnt = closed_retry_count.get(cid, 0)
            if retry_cnt >= MAX_RETRY_ON_CLOSED:
                with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                    f.write(f"{cid}\n")
                async with global_lock:
                    global_completed += 1
                    worker_completed_count[worker_id] += 1
                    worker_last_progress[worker_id] = time.time()
                print(f"[W{worker_id}] 放弃 {cid}: 页面关闭重试已达上限 {MAX_RETRY_ON_CLOSED} 次", flush=True)
                i += 1
                continue

            try:
                school_str, class_str = await process_cid_with_retry(page, cid)
                completed_cids.add(cid)
                async with global_lock:
                    global_completed += 1
                    worker_completed_count[worker_id] += 1
                    worker_last_progress[worker_id] = time.time()
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
                    print(f"[W{worker_id}] 页面关闭 (cid={cid}, 重试 {closed_retry_count[cid]}/{MAX_RETRY_ON_CLOSED})，正在重建...", flush=True)
                    try:
                        await ensure_page()
                    except Exception as rebuild_err:
                        print(f"[W{worker_id}] 重建页面失败: {rebuild_err}", flush=True)
                        if "browser" in str(rebuild_err).lower():
                            break
                    continue
                else:
                    with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                        f.write(f"{cid}\n")
                    async with global_lock:
                        global_completed += 1
                        worker_completed_count[worker_id] += 1
                        worker_last_progress[worker_id] = time.time()
                    error_line = error_msg.split("\n")[0][:100]
                    print(f"[W{worker_id}] 错误 {cid}: {error_line}", flush=True)
                    i += 1

            await asyncio.sleep(SLEEP_BETWEEN)

    except asyncio.CancelledError:
        # 被主进程主动取消（僵死重启）
        remaining = [c for c in assigned_cids if c not in completed_cids]
        if remaining:
            with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                for c in remaining:
                    f.write(f"{c}\n")
        print(f"[W{worker_id}] 被主进程取消，已写入 {len(remaining)} 个未完成 CID", flush=True)
        # 重新抛出，让上层知道
        raise
    finally:
        if page and not page.is_closed():
            await page.close()
        if context:
            await context.close()

async def heartbeat(tasks, worker_count):
    while not stop_event.is_set():
        await asyncio.sleep(10)
        active = sum(1 for t in tasks if not t.done())
        print(f"[心跳] 已完成 {global_completed}/{global_total} | 活跃任务: {active}/{worker_count} | 内存: {get_system_memory_str()} | CPU: {get_cpu_percent_str()}%", flush=True)

async def worker_monitor(tasks, worker_chunks, browser):
    """监控Worker健康，自动重启僵死Worker"""
    while not stop_event.is_set():
        await asyncio.sleep(WORKER_STUCK_SECONDS // 2)  # 每半周期检查
        now = time.time()
        for idx, task in enumerate(tasks[:]):  # 复制列表遍历
            if task.done():
                continue
            worker_id = idx  # 假设索引与worker_id一致
            last_time = worker_last_progress.get(worker_id, 0)
            elapsed = now - last_time
            if elapsed > WORKER_STUCK_SECONDS and worker_completed_count.get(worker_id, 0) < len(worker_chunks[worker_id]):
                print(f"[监控] Worker {worker_id} 已停滞 {elapsed:.0f} 秒，准备重启", flush=True)
                # 取消旧任务
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=10)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                # 读取该worker未完成的CID
                unfinished_file = f"unfinished_worker_{worker_id}.txt"
                remaining_cids = []
                if os.path.exists(unfinished_file):
                    with open(unfinished_file, "r") as f:
                        remaining_cids = [int(line.strip()) for line in f if line.strip()]
                    os.remove(unfinished_file)
                else:
                    # 如果没有文件，则从原始分配的CID中减去已完成数量（保守估计）
                    # 但已完成数量可能不准，直接使用原始分配列表（会重复扫描已完成的，但可接受）
                    remaining_cids = worker_chunks[worker_id][:]
                if remaining_cids:
                    # 创建新Worker
                    new_task = asyncio.create_task(worker(browser, remaining_cids, worker_id))
                    tasks[idx] = new_task
                    # 更新状态
                    worker_last_progress[worker_id] = time.time()
                    worker_completed_count[worker_id] = 0
                    print(f"[监控] Worker {worker_id} 已重启，剩余 {len(remaining_cids)} 个CID", flush=True)
                else:
                    # 没有剩余CID，该worker可以结束
                    tasks[idx] = None
                    print(f"[监控] Worker {worker_id} 无剩余CID，已移除", flush=True)

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
    print(f"Worker卡死阈值: {WORKER_STUCK_SECONDS} 秒")
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

        worker_count = min(MAX_CONCURRENT, len(cid_list))
        chunk_size = (len(cid_list) + worker_count - 1) // worker_count
        chunks = [cid_list[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count)]
        # 记录每个worker分配的CID列表
        for i, chunk in enumerate(chunks):
            worker_assigned_cids[i] = chunk
            worker_last_progress[i] = time.time()
            worker_completed_count[i] = 0

        tasks = [asyncio.create_task(worker(browser, chunk, i)) for i, chunk in enumerate(chunks)]

        # 启动监控任务
        monitor_task = asyncio.create_task(worker_monitor(tasks, chunks, browser))
        heartbeat_task = asyncio.create_task(heartbeat(tasks, worker_count))

        async def set_stop():
            await asyncio.sleep(TIMEOUT_SECONDS)
            stop_event.set()
            print(f"\n⚠️ 已达到软超时（{TIMEOUT_HOURS}小时），停止接收新班级，等待现有任务完成（最多 {FORCE_EXIT_WAIT} 秒）...", flush=True)

        soft_timeout_task = asyncio.create_task(set_stop())

        hard_timeout = TIMEOUT_SECONDS + FORCE_EXIT_WAIT + 30
        try:
            done, pending = await asyncio.wait_for(
                asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED),
                timeout=hard_timeout
            )
            if pending:
                print(f"硬超时内仍有 {len(pending)} 个未完成，取消它们", flush=True)
                for t in pending:
                    t.cancel()
                await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=10)
                stop_event.set()
        except asyncio.TimeoutError:
            print(f"致命超时（总时间 > {hard_timeout}s），强制终止所有任务", flush=True)
            for t in tasks:
                t.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)
            except asyncio.TimeoutError:
                print("强制清理超时，直接退出进程", flush=True)
                os._exit(1)
            stop_event.set()
        finally:
            soft_timeout_task.cancel()
            monitor_task.cancel()
            heartbeat_task.cancel()
            await flush_buffer()
            await browser.close()

    elapsed_total = time.time() - start_time

    if stop_event.is_set():
        with open(UNFINISHED_FLAG, "w") as f:
            f.write(f"Timeout at {time.ctime()}\n")
            f.write(f"Processed {global_completed}/{global_total}\n")
        print(f"\n⚠️ 扫描未完成，已生成 {UNFINISHED_FLAG}", flush=True)

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
