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
MAX_CONCURRENT = config.get("max_concurrent_pages", 20)
WAIT_TIMEOUT = config.get("wait_timeout", 15)
RENDER_WAIT = config.get("render_wait", 0.5)
SLEEP_BETWEEN = config.get("sleep_between", 0.1)
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

# ========== 进程池初始化 ==========
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

# ---------- Worker 同步函数 ----------
def worker_sync(worker_id, config_dict, proxy_list, user_agents, deadline):
    global global_task_queue
    task_queue = global_task_queue

    time.sleep(worker_id * 1.5)
    print(f"[Worker {worker_id}] 正在启动浏览器引擎...", flush=True)

    WAIT_TIMEOUT = config_dict.get("wait_timeout", 25)
    RENDER_WAIT = config_dict.get("render_wait", 2.0)
    SLEEP_BETWEEN = config_dict.get("sleep_between", 0.1)
    MAX_RETRY_ON_CLOSED = config_dict.get("max_retry_on_closed", 3)
    BATCH_SIZE = config_dict.get("batch_size", 100)

    worker_out_file = f"data/worker_{worker_id}_temp.txt"
    unfin_file = f"unfinished_worker_{worker_id}.txt"
    working_file = f"data/working_{worker_id}.txt"
    
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
            
    def clear_working_flag():
        if os.path.exists(working_file):
            try: os.remove(working_file)
            except: pass

    def cleanup():
        nonlocal browser, context, page
        try:
            if page: page.close()
            if context: context.close()
            if browser: browser.close()
        except: pass

    # 【核心修复1】：强制挂载拦截器的安全开页函数，杜绝僵尸进程产生
    def get_fast_page(ctx):
        new_p = ctx.new_page()
        new_p.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font", "stylesheet"] else route.continue_())
        return new_p

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
            page = get_fast_page(context)

            closed_retry = {}
            current_cid = None
            lifecycle_count = 0
            consecutive_errors = 0
            MAX_LIFECYCLE = 200

            while True:
                remaining_time = deadline - time.time()
                if remaining_time <= 0:
                    if current_cid is not None:
                        with open(unfin_file, "a", encoding="utf-8") as f:
                            f.write(f"{current_cid}\n")
                        clear_working_flag()
                    break

                if current_cid is None:
                    try:
                        current_cid = task_queue.get_nowait()
                        with open(working_file, "w", encoding="utf-8") as f:
                            f.write(str(current_cid))
                    except queue.Empty:
                        break

                if closed_retry.get(current_cid, 0) >= MAX_RETRY_ON_CLOSED:
                    with open(unfin_file, "a", encoding="utf-8") as f:
                        f.write(f"{current_cid}\n")
                    clear_working_flag()
                    increment_progress() 
                    current_cid = None
                    continue

                try:
                    pw_timeout = min(WAIT_TIMEOUT * 1000, remaining_time * 1000)
                    page.goto(f"https://www.eeo.cn/s/a/?cid={current_cid}", timeout=pw_timeout, wait_until="domcontentloaded")
                    
                    title = page.title() or ""
                    body_text = page.text_content("body") or ""
                    
                    # 【核心增强】：深度 WAF 风控侦测 (包含标题和内容的隐性拦截)
                    is_waf = False
                    waf_keywords = ["just a moment", "access denied", "attention required", "security", "403", "404", "拦截", "验证码", "error", "cloudflare", "verify you are human", "滑动验证"]
                    
                    if any(k in title.lower() for k in waf_keywords) or any(k in body_text.lower() for k in ["cloudflare", "verify you are human", "滑动验证"]):
                        is_waf = True
                        
                    if is_waf:
                        if random.random() < 0.1: 
                            print(f"⚠️ [风控侦测] Worker-{worker_id} 遭遇防火墙软拦截！准备强制核爆重置 IP 会话...", flush=True)
                        raise Exception("WAF_BLOCKED_FORCED_RESET")
                        
                    if len(body_text.strip()) < 50:
                        school, class_name = "无", "无"
                    else:
                        render_timeout = min(1500, remaining_time * 1000)
                        try: page.wait_for_selector("p.courseName, p.schoolName", timeout=render_timeout)
                        except: pass
                        
                        class_name = "无"
                        for selector in ["p.courseName", ".courseName", "h1", ".title"]:
                            elem = page.query_selector(selector)
                            if elem:
                                text = (elem.text_content() or "").strip()
                                text = " ".join(text.split())
                                if text and len(text) >= 2:
                                    class_name = text
                                    break
                                    
                        if class_name == "无":
                            if title and "Join the class" not in title and "eeo.cn" not in title:
                                if "|" in title: class_name = title.split("|")[-1].strip()
                                elif "-" in title: class_name = title.split("-")[0].strip()

                        school = "无"
                        for selector in ["p.schoolName", ".schoolName", ".orgName"]:
                            elem = page.query_selector(selector)
                            if elem:
                                text = (elem.text_content() or "").strip()
                                text = " ".join(text.split())
                                if text and len(text) >= 2:
                                    school = text
                                    break

                    if not (class_name == "无" and school == "无"):
                        line = f"{current_cid} https://www.eeo.cn/s/a/?cid={current_cid} {school} {class_name}\n"
                        add_line(line)
                        print(f"✅ [发现班级] Worker-{worker_id} | CID: {current_cid} | 学校: {school} | 班级: {class_name}", flush=True)

                    if current_cid in closed_retry: del closed_retry[current_cid]
                    increment_progress() 
                    clear_working_flag()
                    current_cid = None
                    lifecycle_count += 1 
                    consecutive_errors = 0  # 成功后清零错误计数器

                except Exception as e:
                    err_msg = str(e).lower()
                    is_closed = any(phrase in err_msg for phrase in ["closed", "context", "browser", "target closed"])
                    is_timeout = "timeout" in err_msg
                    is_waf_error = "waf_blocked" in err_msg
                    
                    # 记录未完成数据
                    with open(unfin_file, "a", encoding="utf-8") as f:
                        f.write(f"{current_cid}\n")
                        
                    consecutive_errors += 1
                    
                    # 【核心修复 2】：连续错误或 WAF 拦截时，触发“核爆级别”的浏览器重建，清理毒瘤状态
                    if is_closed or is_timeout or is_waf_error or consecutive_errors >= 2:
                        cleanup()
                        if is_waf_error or consecutive_errors >= 3:
                            time.sleep(3) # 触发网络避险冷却
                        
                        browser = p.chromium.launch(headless=True, args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"])
                        context = browser.new_context(user_agent=random.choice(user_agents), ignore_https_errors=True)
                        page = get_fast_page(context)
                        consecutive_errors = 0
                    else:
                        try: page.close()
                        except: pass
                        # 此处如果只建新页面，必须调用封装好的带有拦截器的创建函数！
                        try: page = get_fast_page(context)
                        except: pass
                        
                    increment_progress()
                    clear_working_flag()
                    current_cid = None
                    lifecycle_count += 1

                time.sleep(SLEEP_BETWEEN)

                # 定期转生内存清理
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
                    page = get_fast_page(context)
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
    completed_count = mp.Value('i', 0) 
    
    print(f"正在将 {total} 个任务装载入并发队列，请稍候...", flush=True)
    for cid in cid_list:
        task_queue.put(cid)
    print("✅ 队列装载完毕，准备启动引擎！\n", flush=True)

    deadline = time.time() + TIMEOUT_SECONDS - 60

    pool = mp.Pool(processes=worker_count, initializer=init_worker, initargs=(task_queue, completed_count))
    async_results = []
    
    for i in range(worker_count):
        res = pool.apply_async(worker_sync, (i, config_dict, PROXY_LIST, USER_AGENTS, deadline))
        async_results.append(res)

    pool.close()
    
    start_time = time.time()
    last_print_time = start_time
    hard_limit = start_time + TIMEOUT_SECONDS + FORCE_EXIT_WAIT

    while time.time() < hard_limit:
        if all(res.ready() for res in async_results):
            break
        
        now = time.time()
        if now - last_print_time >= 60:
            c = completed_count.value
            elapsed = now - start_time
            if c > 0:
                speed = c / elapsed
                rem = total - c
                eta = rem / speed if speed > 0 else 0
                eta_str = format_time(eta)
            else:
                speed = 0
                eta_str = "计算中..."
            
            pct = (c / total) * 100 if total > 0 else 0
            print(f"\n📊 [全局监控] 已完成: {c}/{total} ({pct:.2f}%) | ⚡ 速度: {speed:.1f} 个/秒 | ⏳ 剩余时间: {eta_str}\n", flush=True)
            last_print_time = now

        time.sleep(1)

    if not all(res.ready() for res in async_results):
        pool.terminate()
    pool.join()

    # --- 数据合并与清洗 ---
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

    # --- 收集未完成数据 (包含被拔电源死亡瞬间拿着的凭证) ---
    unfinished_cids = set()
    
    for i in range(worker_count):
        ufile = f"unfinished_worker_{i}.txt"
        if os.path.exists(ufile):
            with open(ufile, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().isdigit():
                        unfinished_cids.add(int(line.strip()))
            os.remove(ufile)
            
        working_file = f"data/working_{i}.txt"
        if os.path.exists(working_file):
            with open(working_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content.isdigit():
                    unfinished_cids.add(int(content))
            os.remove(working_file)

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
        print(f"记录了 {len(unfinished_cids)} 个未完成的 CID，已生成补扫信标 ({UNFINISHED_FLAG})", flush=True)
    else:
        print("所有CID已成功处理", flush=True)

    print("✅ 引擎安全退出，释放所有底层资源。", flush=True)
    sys.stdout.flush()
    os._exit(0)

if __name__ == "__main__":
    mp.freeze_support()
    main()
