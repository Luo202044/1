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

# ========== 进程池初始化 ==========
global_task_queue = None
global_counter = None
global_lock = None

def init_worker(q, counter, lock):
    """
    将队列、共享计数器和锁挂载到 Worker 的独立内存中
    """
    global global_task_queue, global_counter, global_lock
    global_task_queue = q
    global_counter = counter
    global_lock = lock

# ---------- Worker 同步函数 (动态抢单模式) ----------
def worker_sync(worker_id, config_dict, proxy_list, user_agents, deadline):
    global global_task_queue, global_counter, global_lock
    task_queue = global_task_queue

    time.sleep(worker_id * 1.5)
    print(f"[Worker {worker_id}] 正在启动浏览器引擎...", flush=True)

    WAIT_TIMEOUT = config_dict.get("wait_timeout", 25)
    RENDER_WAIT = config_dict.get("render_wait", 2.0)
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

    def mark_done():
        """原子的方式增加进度条"""
        with global_lock:
            global_counter.value += 1

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

            while True:
                # 软超时处理
                if time.time() > deadline:
                    print(f"[Worker {worker_id}] 触发软超时，准备下班...", flush=True)
                    if current_cid is not None:
                        with open(unfin_file, "a", encoding="utf-8") as f:
                            f.write(f"{current_cid}\n")
                        mark_done() # 超时抛弃也算处理完毕该任务
                    break

                # 抢单
                if current_cid is None:
                    try:
                        current_cid = task_queue.get_nowait()
                    except queue.Empty:
                        break

                # 坏死链接抛弃
                if closed_retry.get(current_cid, 0) >= MAX_RETRY_ON_CLOSED:
                    with open(unfin_file, "a", encoding="utf-8") as f:
                        f.write(f"{current_cid}\n")
                    current_cid = None
                    mark_done() # 坏死链接记录完毕，算作进度+1
                    continue

                # 执行扫描
                try:
                    page.goto(f"https://www.eeo.cn/s/a/?cid={current_cid}", timeout=WAIT_TIMEOUT*1000, wait_until="domcontentloaded")
                    body = page.text_content("body") or ""
                    
                    if len(body.strip()) < 50:
                        school, class_name = "无", "无"
                    else:
                        try: page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT*1000)
                        except: pass
                        
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
                        # 为减少日志刷屏，取消成功抓取的打印，让主进程进度条发挥作用
                        # print(f"[Worker {worker_id}] CID: {current_cid}, 学校: {school}, 班级: {class_name}", flush=True)

                    if current_cid in closed_retry: del closed_retry[current_cid]
                    current_cid = None
                    mark_done() # 成功抓取！进度+1

                except Exception as e:
                    err_msg = str(e).lower()
                    is_closed = any(phrase in err_msg for phrase in ["closed", "context", "browser"])
                    
                    if is_closed:
                        # 浏览器闭合崩溃：增加重试次数，【不增加进度】，下一次循环继续扫它
                        closed_retry[current_cid] = closed_retry.get(current_cid, 0) + 1
                        cleanup()
                        browser = p.chromium.launch(headless=True, args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"])
                        context = browser.new_context(user_agent=random.choice(user_agents), ignore_https_errors=True)
                        page = context.new_page()
                    else:
                        # 其他异常抛弃
                        with open(unfin_file, "a", encoding="utf-8") as f:
                            f.write(f"{current_cid}\n")
                        
                        try: page.close()
                        except: pass
                        try: page = context.new_page()
                        except: pass
                        
                        current_cid = None
                        mark_done() # 异常抛弃记录完毕，进度+1

                time.sleep(SLEEP_BETWEEN)

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
        if START_CID is None or END_CID is None:
            print("错误: 必须指定 start_cid/end_cid 或 cid_list_file", flush=True)
            sys.exit(1)
        if START_CID > END_CID:
            START_CID, END_CID = END_CID, START_CID
        cid_list = list(range(START_CID, END_CID + 1))
        total = len(cid_list)
        print(f"区间模式: {START_CID} ~ {END_CID} (共 {total} 个)")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f: f.write("")

    config_dict = {
        "wait_timeout": WAIT_TIMEOUT, "render_wait": RENDER_WAIT,
        "sleep_between": SLEEP_BETWEEN, "max_retry_on_closed": MAX_RETRY_ON_CLOSED,
        "batch_size": BATCH_SIZE,
    }

    worker_count = min(MAX_CONCURRENT, len(cid_list))
    task_queue = mp.Queue()
    completed_counter = mp.Value('i', 0)
    counter_lock = mp.Lock()
    
    print(f"正在将 {total} 个任务装载入并发队列，请稍候...", flush=True)
    for cid in cid_list:
        task_queue.put(cid)
    print("✅ 队列装载完毕，准备启动引擎！", flush=True)

    deadline = time.time() + TIMEOUT_SECONDS - 60
    start_time = time.time()
    last_print_time = start_time

    pool = mp.Pool(processes=worker_count, initializer=init_worker, initargs=(task_queue, completed_counter, counter_lock))
    async_results = []
    
    for i in range(worker_count):
        res = pool.apply_async(worker_sync, (i, config_dict, PROXY_LIST, USER_AGENTS, deadline))
        async_results.append(res)

    pool.close()
    
    hard_limit = time.time() + TIMEOUT_SECONDS + FORCE_EXIT_WAIT
    
    # === 主进程进度监工 ===
    print("\n" + "="*50)
    print("🚀 开始高并发扫描，每 5 秒汇报一次进度...")
    print("="*50 + "\n")
    
    while time.time() < hard_limit:
        if all(res.ready() for res in async_results):
            break
            
        current_time = time.time()
        # 每隔 5 秒计算并打印一次进度
        if current_time - last_print_time >= 5.0:
            completed = completed_counter.value
            elapsed = current_time - start_time
            speed = completed / elapsed if elapsed > 0 else 0
            remaining_tasks = total - completed
            eta_seconds = remaining_tasks / speed if speed > 0 else 0
            
            percent = (completed / total) * 100 if total > 0 else 100
            
            # 格式化 ETA 时间 (HH:MM:SS)
            h = int(eta_seconds // 3600)
            m = int((eta_seconds % 3600) // 60)
            s = int(eta_seconds % 60)
            eta_str = f"{h:02d}:{m:02d}:{s:02d}"
            
            # 打印优雅的单行进度条
            bar_len = 30
            filled_len = int(bar_len * completed // total) if total > 0 else bar_len
            bar = '█' * filled_len + '-' * (bar_len - filled_len)
            
            print(f"📊 进度: |{bar}| {percent:.1f}% ({completed}/{total}) | ⚡ 速度: {speed:.1f} 个/秒 | ⏳ 预计剩余: {eta_str}", flush=True)
            last_print_time = current_time
            
        time.sleep(1)

    if not all(res.ready() for res in async_results):
        print("警告: 存在未能响应超时的僵死 Worker，执行强制终止", flush=True)
        pool.terminate()
    pool.join()

    # --- 数据合并与清洗 ---
    print("\n正在合并并清洗最终数据...", flush=True)
    all_valid = []
    for i in range(worker_count):
        temp_file = f"data/worker_{i}_temp.txt"
        if os.path.exists(temp_file):
            with open(temp_file, "r", encoding="utf-8") as f:
                all_valid.extend(f.readlines())
            os.remove(temp_file)

    seen = set()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for line in all_valid:
            parts = line.strip().split()
            if not parts: continue
            cid = parts[0]
            if cid not in seen:
                seen.add(cid)
                f.write(line)

    # --- 收集未完成数据 ---
    unfinished_cids = set()
    
    for i in range(worker_count):
        ufile = f"unfinished_worker_{i}.txt"
        if os.path.exists(ufile):
            with open(ufile, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().isdigit():
                        unfinished_cids.add(int(line.strip()))
            os.remove(ufile)

    while not task_queue.empty():
        try:
            unfinished_cids.add(task_queue.get_nowait())
        except queue.Empty:
            break

    if unfinished_cids:
        with open(f"unfinished_cids_{SHARD_IDX}.txt", "w", encoding="utf-8") as f:
            for cid in sorted(unfinished_cids):
                f.write(f"{cid}\n")
        open(UNFINISHED_FLAG, "w").close() 
        print(f"✅ 记录了 {len(unfinished_cids)} 个未完成的 CID，已生成补扫信标 ({UNFINISHED_FLAG})", flush=True)
    else:
        print("✅ 所有CID已成功处理，无遗漏！", flush=True)

if __name__ == "__main__":
    mp.freeze_support()
    main()
