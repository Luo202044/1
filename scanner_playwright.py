#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import random
import atexit
import queue
import traceback
import multiprocessing as mp
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

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
MAX_CONCURRENT = config.get("max_concurrent_pages", 15)
WAIT_TIMEOUT = config.get("wait_timeout", 20)
RENDER_WAIT = config.get("render_wait", 1.0)
SLEEP_BETWEEN = config.get("sleep_between", 0.6)
RETRY_TIMES = config.get("retry_times", 1)
RETRY_DELAY = config.get("retry_delay", 0.3)
TIMEOUT_HOURS = config.get("timeout_hours", 5.0)
TIMEOUT_SECONDS = TIMEOUT_HOURS * 3600
FORCE_EXIT_WAIT = config.get("force_exit_wait", 300)
BATCH_SIZE = config.get("batch_size", 100)
MAX_RETRY_ON_CLOSED = config.get("max_retry_on_closed", 3)

os.makedirs("data", exist_ok=True)

if START_CID is not None and END_CID is not None and not CID_LIST_FILE:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "") if CID_LIST_FILE else "unknown"
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

PROXY_LIST = []
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def format_time(seconds):
    if seconds < 0: return "0s"
    if seconds < 60: return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

# ========== 进程池初始化 (队列与进度计数器) ==========
global_task_queue = None
global_completed_count = None

def init_worker(q, counter):
    global global_task_queue, global_completed_count
    global_task_queue = q
    global_completed_count = counter

def increment_progress():
    global global_completed_count
    if global_completed_count is not None:
        with global_completed_count.get_lock():
            global_completed_count.value += 1

# ---------- Worker 同步函数 (动态抢单模式) ----------
def worker_sync(worker_id, config_dict, proxy_list, user_agents, deadline):
    global global_task_queue
    task_queue = global_task_queue

    time.sleep(worker_id * 1.5)
    print(f"[Worker {worker_id}] 正在启动浏览器引擎...", flush=True)

    WAIT_TIMEOUT = config_dict.get("wait_timeout", 25)
    SLEEP_BETWEEN = config_dict.get("sleep_between", 0.3)
    MAX_RETRY_ON_CLOSED = config_dict.get("max_retry_on_closed", 3)
    BATCH_SIZE = config_dict.get("batch_size", 100)

    worker_out_file = f"data/worker_{worker_id}_temp.txt"
    unfin_file = f"unfinished_worker_{worker_id}.txt"
    write_buffer = []
    browser = None
    context = None
    page = None

    def flush():
        if write_buffer:
            with open(worker_out_file, "a", encoding="utf-8") as f:
                f.write("".join(write_buffer))
            write_buffer.clear()

    def add_line(line):
        write_buffer.append(line)
        if len(write_buffer) >= BATCH_SIZE:
            flush()

    def cleanup():
        nonlocal browser, context, page
        try:
            if page: page.close()
            if context: context.close()
            if browser: browser.close()
        except: pass

    atexit.register(cleanup)
    
    try:
        try: os.setpgrp()
        except: pass

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"])
            context = browser.new_context(
                user_agent=random.choice(user_agents),
                proxy={"server": random.choice(proxy_list)} if proxy_list else None,
                ignore_https_errors=True
            )
            page = context.new_page()

            closed_retry = {}
            current_cid = None
            
            lifecycle_count = 0
            MAX_LIFECYCLE = 200

            while True:
                if time.time() > deadline:
                    print(f"[Worker {worker_id}] 触发软超时，准备下班...", flush=True)
                    if current_cid is not None:
                        with open(unfin_file, "a", encoding="utf-8") as f:
                            f.write(f"{current_cid}\n")
                    break

                if current_cid is None:
                    try:
                        current_cid = task_queue.get_nowait()
                    except queue.Empty:
                        break

                if closed_retry.get(current_cid, 0) >= MAX_RETRY_ON_CLOSED:
                    with open(unfin_file, "a", encoding="utf-8") as f:
                        f.write(f"{current_cid}\n")
                    increment_progress() 
                    current_cid = None
                    continue

                try:
                    page.goto(f"https://www.eeo.cn/s/a/?cid={current_cid}", timeout=WAIT_TIMEOUT*1000, wait_until="domcontentloaded")
                    
                    # 【核心修复】：删除 body<50 的判断。直接强制等待目标元素，给予最高 3 秒宽限期。
                    # 如果页面有效，几乎会瞬间拿到元素；如果是死链，只会惩罚 3 秒的时间。绝对不漏数据。
                    try: 
                        page.wait_for_selector("p.courseName, p.schoolName", timeout=3000)
                    except: 
                        pass
                    
                    class_name = "无"
                    elem = page.query_selector("p.courseName")
                    if elem:
                        text = elem.inner_text().strip()
                        if text and len(text) >= 2: class_name = text
                    
                    if class_name == "无":
                        title = page.title()
                        if "|" in title and "Join the class" not in title:
                            parts = title.split("|")
                            if len(parts) > 1: class_name = parts[-1].strip()
                    
                    school = "无"
                    elem = page.query_selector("p.schoolName")
                    if elem:
                        text = elem.inner_text().strip()
                        if text and len(text) >= 2: school = text

                    if not (class_name == "无" and school == "无"):
                        line = f"{current_cid} https://www.eeo.cn/s/a/?cid={current_cid} {school} {class_name}\n"
                        add_line(line)
                        # 【核心修复】：恢复打印，并加上显眼的绿色勾号
                        print(f"✅ [Worker {worker_id}] 捕获 -> CID: {current_cid} | {school} | {class_name}", flush=True)

                    if current_cid in closed_retry: del closed_retry[current_cid]
                    increment_progress() 
                    current_cid = None
                    lifecycle_count += 1 

                except Exception as e:
                    err_msg = str(e).lower()
                    is_closed = any(phrase in err_msg for phrase in ["closed", "context", "browser"])
                    
                    if is_closed:
                        closed_retry[current_cid] = closed_retry.get(current_cid, 0) + 1
                        cleanup()
                        browser = p.chromium.launch(headless=True, args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"])
                        context = browser.new_context(user_agent=random.choice(user_agents), ignore_https_errors=True)
                        page = context.new_page()
                    else:
                        with open(unfin_file, "a", encoding="utf-8") as f:
                            f.write(f"{current_cid}\n")
                        
                        try: page.close()
                        except: pass
                        try: page = context.new_page()
                        except: pass
                        
                        increment_progress() 
                        current_cid = None
                        lifecycle_count += 1 

                time.sleep(SLEEP_BETWEEN)

                if lifecycle_count >= MAX_LIFECYCLE:
                    try: page.close()
                    except: pass
                    try: context.close()
                    except: pass
                    
                    context = browser.new_context(
                        user_agent=random.choice(user_agents),
                        proxy={"server": random.choice(proxy_list)} if proxy_list else None,
                        ignore_https_errors=True
                    )
                    page = context.new_page()
                    lifecycle_count = 0  

            flush()
            
    except Exception as e:
        print(f"\n❌ [Worker {worker_id}] 发生致命崩溃: {str(e)}\n{traceback.format_exc()}\n", flush=True)
    finally:
        cleanup()
    return worker_id

# ---------- 主函数 ----------
def main():
    global START_CID, END_CID, CID_LIST_FILE, OUTPUT_FILE, SHARD_IDX, UNFINISHED_FLAG

    if CID_LIST_FILE:
        with open(CID_LIST_FILE, "r", encoding="utf-8") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
        total = len(cid_list)
        if total == 0:
            print("错误: CID列表文件为空", flush=True)
            sys.exit(1)
        print(f"列表模式: 从 {CID_LIST_FILE} 读取 {total} 个班级")
    else:
        if START_
