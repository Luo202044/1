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
# 代理列表（请替换为真实代理地址，支持 http://, https://, socks5://）
# 如果列表为空，则不使用代理
PROXY_LIST = [
    # "http://127.0.0.1:8080",
    # "socks5://user:pass@proxy.example.com:1080",
]

# 预定义的 User-Agent 列表（覆盖常见操作系统 + 浏览器组合）
USER_AGENTS = [
    # Windows 10 + Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Windows 10 + Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    # Windows 10 + Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    # macOS + Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # macOS + Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    # macOS + Firefox
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
    # Linux + Chrome
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Linux + Firefox
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    # Android + Chrome Mobile
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    # Android + Firefox Mobile
    "Mozilla/5.0 (Android 13; Mobile; rv:109.0) Gecko/119.0 Firefox/119.0",
    # iOS + Safari Mobile
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    # iOS + Chrome Mobile
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.119 Mobile/15E148 Safari/604.1",
]

# ========== 配置加载 ==========
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

START_CID = config["start_cid"]
END_CID = config["end_cid"]
MAX_CONCURRENT = config.get("max_concurrent", config.get("threads", 30))
MAX_TASKS_PER_CONTEXT = config.get("max_tasks_per_context", 20)  # 本脚本未严格按此阈值重启，但保留备用
WAIT_TIMEOUT = config.get("wait_timeout", 5)
RENDER_WAIT = config.get("render_wait", 0.2)
SLEEP_BETWEEN = config.get("sleep_between", 0.02)

os.makedirs("data", exist_ok=True)
OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")

global_total = END_CID - START_CID + 1
global_completed = 0
global_lock = Lock()
start_time = None

# ========== 内存监控（psutil）==========
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

async def process_cid(browser, cid, semaphore, total_tasks, thread_name="async"):
    global global_completed, start_time
    async with semaphore:
        # 随机选择 User-Agent 和代理
        user_agent = random.choice(USER_AGENTS)
        proxy = None
        if PROXY_LIST:
            proxy_server = random.choice(PROXY_LIST)
            proxy = {"server": proxy_server}
        
        # 创建上下文（每个班级独立上下文，方便设置不同的 UA 和代理）
        context = await browser.new_context(
            user_agent=user_agent,
            proxy=proxy,
            ignore_https_errors=True,
            java_script_enabled=True
        )
        page = await context.new_page()
        url = f"https://www.eeo.cn/s/a/?cid={cid}"
        try:
            await page.goto(url, timeout=WAIT_TIMEOUT * 1000)
            await page.wait_for_selector("body", timeout=WAIT_TIMEOUT * 1000)
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
            # 在日志中显示使用的 UA 和代理（可选，便于调试）
            proxy_hint = proxy["server"] if proxy else "无代理"
            print(f"[{thread_name}] (进度：{cur_global}/{global_total}) (内存: {mem_str}) (剩余：{remain_str}) {cid} | 机构: {school_str} | 班级: {class_str} | UA: {user_agent[:30]}... | 代理: {proxy_hint}", flush=True)

            if not (class_str == "无" and school_str == "无"):
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{cid} {url} {school_str} {class_str}\n")
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
                print(f"[{thread_name}] (进度：{cur_global}/{global_total}) (内存: {mem_str}) (剩余：{remain_str}) {cid} 超时", flush=True)
            else:
                print(f"[{thread_name}] (进度：{cur_global}/{global_total}) (内存: {mem_str}) (剩余：{remain_str}) {cid} 错误: {e}", flush=True)
            return False
        finally:
            await page.close()
            await context.close()
            await asyncio.sleep(SLEEP_BETWEEN)

async def main_async():
    global start_time
    start_time = time.time()
    print(f"班级范围: {START_CID} - {END_CID}")
    print(f"最大并发数: {MAX_CONCURRENT}")
    print(f"代理数量: {len(PROXY_LIST)} (已启用)" if PROXY_LIST else "代理: 未启用")
    print(f"User-Agent 池大小: {len(USER_AGENTS)}")
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
