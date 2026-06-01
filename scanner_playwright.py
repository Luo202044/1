#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import random
from asyncio import Lock
from playwright.async_api import async_playwright

# ========== 用户配置 ==========
PROXY_LIST = []   # 如需代理请填写 ["http://user:pass@ip:port"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ========== 超时设置（5小时30分钟 = 19800秒）==========
TIMEOUT_SECONDS = 5.5 * 3600   # 19800
stop_flag = False

# ========== 加载配置 ==========
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

START_CID = config["start_cid"]
END_CID = config["end_cid"]
MAX_CONCURRENT = config.get("max_concurrent", 80)      # worker 数量
WAIT_TIMEOUT = config.get("wait_timeout", 10)
RENDER_WAIT = config.get("render_wait", 0.3)
SLEEP_BETWEEN = config.get("sleep_between", 0.01)
RETRY_TIMES = config.get("retry_times", 1)
RETRY_DELAY = config.get("retry_delay", 0.5)

BATCH_SIZE = 200   # 批量写入行数

os.makedirs("data", exist_ok=True)
OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")

global_total = END_CID - START_CID + 1
global_completed = 0
global_lock = Lock()
write_buffer = []
buffer_lock = Lock()
start_time = None

# ---------- 资源监控 ----------
try:
    import psutil
    PSUTIL_AVAILABLE = True
    TOTAL_MEM_GB = psutil.virtual_memory().total / (1024 ** 3)
except ImportError:
    PSUTIL_AVAILABLE = False
    TOTAL_MEM_GB = None

def get_system_memory_str():
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        mem = psutil.virtual_memory()
        used_gb = mem.used / (1024 ** 3)
        return f"{used_gb:.2f}GB/{TOTAL_MEM_GB:.1f}GB"
    except Exception:
        return "?GB/?GB"

def get_cpu_percent_str():
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        cpu = psutil.cpu_percent(interval=0)
        return f"{cpu:.1f}"
    except Exception:
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
        await page.goto(url, timeout=WAIT_TIMEOUT * 1000, wait_until="domcontentloaded")
        await page.wait_for_selector("body", timeout=WAIT_TIMEOUT * 1000)
        try:
            await page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT * 1000)
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
    except Exception as e:
        raise e

async def process_cid_with_retry(page, cid, retry=0):
    try:
        school_str, class_str = await fetch_page_data(page, cid)
        return school_str, class_str
    except Exception as e:
        if retry < RETRY_TIMES and ("timeout" in str(e).lower() or "net::" in str(e).lower()):
            delay = RETRY_DELAY * (2 ** retry)
            await asyncio.sleep(delay)
            return await process_cid_with_retry(page, cid, retry + 1)
        else:
            raise e

# ---------- Worker ----------
async def worker(browser, cid_list, worker_id):
    global global_completed, start_time, stop_flag
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        proxy={"server": random.choice(PROXY_LIST)} if PROXY_LIST else None,
        ignore_https_errors=True,
        java_script_enabled=True
    )
    page = await context.new_page()

    for cid in cid_list:
        if stop_flag:
            break

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
            # 无效班级：完全静默，不打印任何内容

        except Exception as e:
            async with global_lock:
                global_completed += 1
                cur = global_completed
            # 错误信息精简输出（仅 CID 和错误简述）
            print(f"[W{worker_id}] 错误 {cid}: {str(e)[:100]}", flush=True)

        await asyncio.sleep(SLEEP_BETWEEN)

    await page.close()
    await context.close()

# ---------- 超时监控 ----------
async def timeout_monitor():
    global stop_flag
    await asyncio.sleep(TIMEOUT_SECONDS)
    if not stop_flag:
        stop_flag = True
        print(f"\n⚠️ 已达到运行时间上限（{TIMEOUT_SECONDS/3600:.1f}小时），正在优雅停止...", flush=True)

# ---------- 主函数 ----------
async def main_async():
    global start_time, stop_flag
    start_time = time.time()

    print(f"班级范围: {START_CID} - {END_CID} (共 {global_total} 个)")
    print(f"Worker 并发数: {MAX_CONCURRENT}")
    print(f"批量写入大小: {BATCH_SIZE}")
    print(f"超时限制: {TIMEOUT_SECONDS/3600:.1f} 小时")
    print(f"结果保存至: {OUTPUT_FILE}")
    print("控制台仅输出有效班级和错误信息。\n")

    # 清空输出文件
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
        all_cids = list(range(START_CID, END_CID + 1))
        worker_count = min(MAX_CONCURRENT, len(all_cids))
        chunk_size = (len(all_cids) + worker_count - 1) // worker_count
        chunks = [all_cids[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count)]

        # 启动超时监控
        timeout_task = asyncio.create_task(timeout_monitor())

        # 启动所有 workers
        tasks = [asyncio.create_task(worker(browser, chunk, i)) for i, chunk in enumerate(chunks) if chunk]
        await asyncio.gather(*tasks, return_exceptions=True)

        timeout_task.cancel()
        try:
            await timeout_task
        except asyncio.CancelledError:
            pass

        await flush_buffer()
        await browser.close()

    elapsed_total = time.time() - start_time
    if stop_flag:
        print(f"\n⏰ 程序因超时自动停止（已运行 {format_time(elapsed_total)}），结果已保存至 {OUTPUT_FILE}")
    else:
        print(f"\n✅ 扫描完成！总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
