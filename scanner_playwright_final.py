#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import sys
import random
from asyncio import Lock
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
WAIT_TIMEOUT = config.get("wait_timeout", 25)
RENDER_WAIT = config.get("render_wait", 2.0)
SLEEP_BETWEEN = config.get("sleep_between", 0.3)
RETRY_TIMES = config.get("retry_times", 2)
RETRY_DELAY = config.get("retry_delay", 1.0)
TIMEOUT_HOURS = config.get("timeout_hours", 5.5)
TIMEOUT_SECONDS = TIMEOUT_HOURS * 3600
FORCE_EXIT_WAIT = config.get("force_exit_wait", 300)
BATCH_SIZE = config.get("batch_size", 200)
MAX_RETRY_ON_CLOSED = config.get("max_retry_on_closed", 3)
WORKER_STUCK_SECONDS = config.get("worker_stuck_seconds", 300)   # Worker 无进展超时（秒）

os.makedirs("data", exist_ok=True)

if START_CID is not None and END_CID is not None:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "")
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

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
worker_last_progress = {}      # worker_id -> 上次完成数量
worker_completed_count = {}    # worker_id -> 已处理数量
worker_assigned_cids = {}      # worker_id -> 分配的CID列表（剩余待处理）
worker_tasks = []              # 存储当前运行的 worker 协程任务

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

# ---------- 页面提取（带独立超时） ----------
async def fetch_page_data(page, cid, timeout_sec):
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    try:
        await asyncio.wait_for(
            page.goto(url, timeout=WAIT_TIMEOUT * 1000, wait_until="domcontentloaded"),
            timeout=timeout_sec
        )
        body = await page.text_content("body") or ""
        if len(body.strip()) < 50:
            return "无", "无"
        try:
            await asyncio.wait_for(
                page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT * 1000),
                timeout=5
            )
        except:
            pass
        # 班级名
        class_name = "无"
        elem = await page.query_selector("p.courseName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                class_name = text
        if class_name == "无":
            title = await page.title()
            if "|" in title and "Join the class" not in title:
                parts = title.split("|")
                if len(parts) > 1:
                    class_name = parts[-1].strip()
        # 学校名
        school_name = "无"
        elem = await page.query_selector("p.schoolName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                school_name = text
        return school_name, class_name
    except asyncio.TimeoutError:
        raise Exception("操作超时")
    except PlaywrightTimeoutError:
        raise Exception("Playwright超时")
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

# ---------- Worker 协程 ----------
async def worker(browser, cid_list, worker_id):
    global global_completed, start_time, worker_last_progress, worker_completed_count
    context = None
    page = None
    closed_retry = {}
    assigned_cids = set(cid_list)
    completed_cids = set()
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
            # 超过重试限制则放弃
            if closed_retry.get(cid, 0) >= MAX_RETRY_ON_CLOSED:
                with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                    f.write(f"{cid}\n")
                async with global_lock:
                    global_completed += 1
                    worker_completed_count[worker_id] += 1
                    worker_last_progress[worker_id] = time.time()
                print(f"[W{worker_id}] 放弃 {cid}: 重试达上限 {MAX_RETRY_ON_CLOSED}", flush=True)
                i += 1
                continue

            try:
                school, class_name = await process_cid_with_retry(page, cid)
                completed_cids.add(cid)
                async with global_lock:
                    global_completed += 1
                    worker_completed_count[worker_id] += 1
                    worker_last_progress[worker_id] = time.time()
                    cur = global_completed

                is_valid = not (class_name == "无" and school == "无")
                if is_valid:
                    elapsed = time.time() - start_time
                    ratio = cur / global_total
                    remain_str = format_time((elapsed / ratio) - elapsed) if ratio > 0 else "未知"
                    mem_str = get_system_memory_str()
                    cpu_str = get_cpu_percent_str()
                    print(f"[W{worker_id}] ({cur}/{global_total}) 内存:{mem_str} cpu:{cpu_str}% 剩余:{remain_str} {cid} | {school} | {class_name}", flush=True)
                    await add_to_buffer(f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school} {class_name}\n")

                if cid in closed_retry:
                    del closed_retry[cid]
                i += 1

            except Exception as e:
                err_msg = str(e).lower()
                is_closed = any(phrase in err_msg for phrase in ["closed", "context", "browser"])
                if is_closed:
                    closed_retry[cid] = closed_retry.get(cid, 0) + 1
                    print(f"[W{worker_id}] 页面关闭 (cid={cid}, 重试 {closed_retry[cid]}/{MAX_RETRY_ON_CLOSED})", flush=True)
                    try:
                        await ensure_page()
                    except Exception as rebuild_err:
                        print(f"[W{worker_id}] 重建页面失败: {rebuild_err}", flush=True)
                        if "browser" in str(rebuild_err).lower():
                            break
                    continue
                else:
                    # 不可恢复错误，放弃
                    with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                        f.write(f"{cid}\n")
                    async with global_lock:
                        global_completed += 1
                        worker_completed_count[worker_id] += 1
                        worker_last_progress[worker_id] = time.time()
                    print(f"[W{worker_id}] 错误 {cid}: {str(e)[:100]}", flush=True)
                    i += 1

            await asyncio.sleep(SLEEP_BETWEEN)

        # 正常结束（非stop_event），若有遗漏则补充
        remaining = [c for c in assigned_cids if c not in completed_cids]
        if remaining:
            with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                for c in remaining:
                    f.write(f"{c}\n")
    except asyncio.CancelledError:
        # 被健康监控取消，写入未完成CID
        remaining = [c for c in assigned_cids if c not in completed_cids]
        if remaining:
            with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                for c in remaining:
                    f.write(f"{c}\n")
        print(f"[W{worker_id}] 被强制取消，已写入 {len(remaining)} 个未完成 CID", flush=True)
        raise
    finally:
        if page and not page.is_closed():
            await page.close()
        if context:
            await context.close()

# ---------- 健康监控 ----------
async def health_monitor(worker_tasks, worker_cid_lists, worker_ids):
    while not stop_event.is_set():
        await asyncio.sleep(WORKER_STUCK_SECONDS // 2)
        now = time.time()
        for idx, task in enumerate(worker_tasks):
            if task.done():
                continue
            wid = worker_ids[idx]
            last = worker_last_progress.get(wid, 0)
            if now - last > WORKER_STUCK_SECONDS and worker_completed_count.get(wid, 0) < len(worker_cid_lists[idx]):
                print(f"[监控] Worker {wid} 已停滞 {(now-last):.0f} 秒，准备重启", flush=True)
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=10)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                # 重新创建 worker 协程，处理其未完成的 CID
                remaining_file = f"unfinished_worker_{wid}.txt"
                remaining_cids = []
                if os.path.exists(remaining_file):
                    with open(remaining_file, "r") as f:
                        remaining_cids = [int(line.strip()) for line in f if line.strip()]
                    os.remove(remaining_file)
                if not remaining_cids:
                    # 如果没有剩余文件，则从原始分配中减去已完成数量（近似）
                    original = worker_cid_lists[idx]
                    completed_count = worker_completed_count.get(wid, 0)
                    if completed_count < len(original):
                        remaining_cids = original[completed_count:]
                if remaining_cids:
                    new_task = asyncio.create_task(worker(browser, remaining_cids, wid))
                    worker_tasks[idx] = new_task
                    worker_last_progress[wid] = time.time()
                    worker_completed_count[wid] = 0
                    print(f"[监控] Worker {wid} 已重启，剩余 {len(remaining_cids)} 个CID", flush=True)
                else:
                    print(f"[监控] Worker {wid} 无剩余CID，已结束", flush=True)

# ---------- 心跳 ----------
async def heartbeat(worker_tasks, worker_count):
    while not stop_event.is_set():
        await asyncio.sleep(10)
        active = sum(1 for t in worker_tasks if not t.done())
        print(f"[心跳] 已完成 {global_completed}/{global_total} | 活跃任务: {active}/{worker_count} | 内存: {get_system_memory_str()} | CPU: {get_cpu_percent_str()}%", flush=True)

# ---------- 主函数 ----------
async def main_async():
    global start_time, global_total, worker_tasks
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
    print(f"Worker无进展判定: {WORKER_STUCK_SECONDS} 秒")
    print(f"结果保存至: {OUTPUT_FILE}\n")

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

        # 分配CID给Worker
        worker_count = min(MAX_CONCURRENT, len(cid_list))
        chunk_size = (len(cid_list) + worker_count - 1) // worker_count
        chunks = [cid_list[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count)]
        worker_ids = list(range(worker_count))
        for i, chunk in enumerate(chunks):
            worker_assigned_cids[i] = chunk
            worker_last_progress[i] = time.time()
            worker_completed_count[i] = 0

        worker_tasks = [asyncio.create_task(worker(browser, chunk, i)) for i, chunk in enumerate(chunks)]

        # 启动监控和心跳
        monitor_task = asyncio.create_task(health_monitor(worker_tasks, chunks, worker_ids))
        heartbeat_task = asyncio.create_task(heartbeat(worker_tasks, worker_count))

        # 软超时
        async def set_stop():
            await asyncio.sleep(TIMEOUT_SECONDS)
            stop_event.set()
            print(f"\n⚠️ 软超时（{TIMEOUT_HOURS}小时），停止新任务，等待现有任务 {FORCE_EXIT_WAIT} 秒...", flush=True)

        soft_timeout_task = asyncio.create_task(set_stop())

        # 硬超时：软超时 + 强制等待
        hard_timeout = TIMEOUT_SECONDS + FORCE_EXIT_WAIT + 30
        try:
            # 等待所有 worker 完成，但设置总超时
            await asyncio.wait_for(asyncio.gather(*worker_tasks, return_exceptions=True), timeout=hard_timeout)
        except asyncio.TimeoutError:
            print(f"硬超时（{hard_timeout}秒），强制取消所有任务", flush=True)
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            stop_event.set()
        finally:
            soft_timeout_task.cancel()
            monitor_task.cancel()
            heartbeat_task.cancel()
            await flush_buffer()
            await browser.close()

    elapsed_total = time.time() - start_time

    # 处理未完成标志
    if stop_event.is_set():
        with open(UNFINISHED_FLAG, "w") as f:
            f.write(f"Timeout at {time.ctime()}\n")
            f.write(f"Processed {global_completed}/{global_total}\n")
        print(f"\n⚠️ 扫描未完成，已生成 {UNFINISHED_FLAG}", flush=True)

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
        if os.path.exists(UNFINISHED_FLAG):
            os.remove(UNFINISHED_FLAG)
        print(f"\n✅ 扫描完成！总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}")

    print(f"最终处理班级数: {global_completed}/{global_total}", flush=True)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
