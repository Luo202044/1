#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import random
from asyncio import Semaphore, Lock
from playwright.async_api import async_playwright

# ========== 用户配置区域 ==========
PROXY_LIST = [
    # "http://127.0.0.1:8080",
]

USER_AGENTS = [
    # ... 保持原有列表 ...
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # ... 其他 UA ...
]

# ========== 加载配置 ==========
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

START_CID = config["start_cid"]
END_CID = config["end_cid"]
MAX_CONCURRENT = config.get("max_concurrent", 80)          # 推荐从80开始
MAX_TASKS_PER_CONTEXT = config.get("max_tasks_per_context", 20)
WAIT_TIMEOUT = config.get("wait_timeout", 8)               # 增加到8秒
RENDER_WAIT = config.get("render_wait", 0.5)               # 增加到0.5秒
SLEEP_BETWEEN = config.get("sleep_between", 0.05)          # 稍微提高间隔
RETRY_TIMES = config.get("retry_times", 2)                 # 重试次数
RETRY_DELAY = config.get("retry_delay", 1.0)               # 初始重试延迟（秒）

os.makedirs("data", exist_ok=True)
OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")

global_total = END_CID - START_CID + 1
global_completed = 0
global_lock = Lock()
start_time = None

# ========== 内存监控 ==========
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

def format_time(seconds):
    if seconds < 0:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

async def fetch_page_data(browser, cid, retry=0):
    """单次请求，返回 (school_str, class_str) 或引发异常"""
    user_agent = random.choice(USER_AGENTS)
    proxy = None
    if PROXY_LIST:
        proxy_server = random.choice(PROXY_LIST)
        proxy = {"server": proxy_server}
    
    context = await browser.new_context(
        user_agent=user_agent,
        proxy=proxy,
        ignore_https_errors=True,
        java_script_enabled=True
    )
    page = await context.new_page()
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    try:
        # 使用 domcontentloaded 更快，不需要等待所有资源
        await page.goto(url, timeout=WAIT_TIMEOUT * 1000, wait_until="domcontentloaded")
        # 等待 body 存在
        await page.wait_for_selector("body", timeout=WAIT_TIMEOUT * 1000)
        # 等待关键元素短时间
        try:
            await page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT * 1000)
        except:
            pass

        # 提取班级名称
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

        # 提取学校名称
        school_name = None
        elem = await page.query_selector("p.schoolName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                school_name = text
        school_str = school_name if school_name else "无"

        await page.close()
        await context.close()
        return school_str, class_str
    except Exception as e:
        await page.close()
        await context.close()
        # 如果是超时或网络错误且还有重试次数，则延迟后重试
        if retry < RETRY_TIMES and ("timeout" in str(e).lower() or "net::" in str(e).lower()):
            delay = RETRY_DELAY * (2 ** retry)  # 指数退避
            await asyncio.sleep(delay)
            return await fetch_page_data(browser, cid, retry+1)
        else:
            raise e

async def process_cid(browser, cid, semaphore, total_tasks, thread_name="async"):
    global global_completed, start_time
    async with semaphore:
        try:
            school_str, class_str = await fetch_page_data(browser, cid)
            async with global_lock:
                global_completed += 1
                cur_global = global_completed
            elapsed = time.time() - start_time
            ratio = cur_global / global_total
            if ratio > 0:
                remaining = (elapsed / ratio) - elapsed
                remain_str = format_time(remaining)
            else:
                remain_str = "未知"
            mem_str = get_system_memory_str()
            print(f"[{thread_name}] (进度：{cur_global}/{global_total}) (内存: {mem_str}) (剩余：{remain_str}) {cid} | 机构: {school_str} | 班级: {class_str}", flush=True)

            if not (class_str == "无" and school_str == "无"):
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school_str} {class_str}\n")
                return True
            return False
        except Exception as e:
            async with global_lock:
                global_completed += 1
                cur_global = global_completed
            elapsed = time.time() - start_time
            ratio = cur_global / global_total if global_total > 0 else 0
            if ratio > 0:
                remaining = (elapsed / ratio) - elapsed
                remain_str = format_time(remaining)
            else:
                remain_str = "未知"
            mem_str = get_system_memory_str()
            if "timeout" in str(e).lower():
                print(f"[{thread_name}] (进度：{cur_global}/{global_total}) (内存: {mem_str}) (剩余：{remain_str}) {cid} 最终超时（已重试{RETRY_TIMES}次）", flush=True)
            else:
                print(f"[{thread_name}] (进度：{cur_global}/{global_total}) (内存: {mem_str}) (剩余：{remain_str}) {cid} 最终错误: {e}", flush=True)
            return False

async def main_async():
    global start_time
    start_time = time.time()
    print(f"班级范围: {START_CID} - {END_CID}")
    print(f"最大并发数: {MAX_CONCURRENT}")
    print(f"代理数量: {len(PROXY_LIST)} (已启用)" if PROXY_LIST else "代理: 未启用")
    print(f"User-Agent 池大小: {len(USER_AGENTS)}")
    print(f"超时设置: {WAIT_TIMEOUT}s, 重试次数: {RETRY_TIMES}")
    total = END_CID - START_CID + 1
    print(f"总班级数: {total}")
    print(f"结果保存至: {OUTPUT_FILE}\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-accelerated-2d-canvas",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-breakpad",
                "--disable-client-side-phishing-detection",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                "--disable-hang-monitor",
                "--disable-ipc-flooding-protection",
                "--disable-popup-blocking",
                "--disable-prompt-on-repost",
                "--disable-renderer-backgrounding",
                "--disable-sync",
                "--disable-software-rasterizer",
                "--metrics-recording-only",
                "--no-first-run",
                "--safebrowsing-disable-auto-update",
                "--disable-logging",
                "--silent",
                "--js-flags=--max-old-space-size=128"
            ]
        )
        semaphore = Semaphore(MAX_CONCURRENT)
        tasks = []
        for cid in range(START_CID, END_CID + 1):
            tasks.append(asyncio.create_task(process_cid(browser, cid, semaphore, total, thread_name="A")))
        results = await asyncio.gather(*tasks)
        valid = sum(1 for r in results if r)

        await browser.close()
        elapsed_total = time.time() - start_time
        print(f"\n探测完成！有效班级数: {valid}，总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
