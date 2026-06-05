#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import random
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

os.makedirs("data", exist_ok=True)

if START_CID is not None and END_CID is not None:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "")
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

PROXY_LIST = []
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

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

# ---------- Worker 同步函数 ----------
def worker_sync(worker_id, cid_list, config_dict, proxy_list, user_agents):
    WAIT_TIMEOUT = config_dict.get("wait_timeout", 25)
    RENDER_WAIT = config_dict.get("render_wait", 2.0)
    SLEEP_BETWEEN = config_dict.get("sleep_between", 0.3)
    RETRY_TIMES = config_dict.get("retry_times", 2)
    RETRY_DELAY = config_dict.get("retry_delay", 1.0)
    MAX_RETRY_ON_CLOSED = config_dict.get("max_retry_on_closed", 3)
    BATCH_SIZE = config_dict.get("batch_size", 200)

    worker_out_file = f"data/worker_{worker_id}_temp.txt"
    unfin_file = f"unfinished_worker_{worker_id}.txt"
    write_buffer = []

    def flush():
        if write_buffer:
            with open(worker_out_file, "a", encoding="utf-8") as f:
                f.write("".join(write_buffer))
            write_buffer.clear()

    def add_line(line):
        write_buffer.append(line)
        if len(write_buffer) >= BATCH_SIZE:
            flush()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
        )
        context = browser.new_context(
            user_agent=random.choice(user_agents),
            proxy={"server": random.choice(proxy_list)} if proxy_list else None,
            ignore_https_errors=True
        )
        page = context.new_page()

        i = 0
        closed_retry = {}
        while i < len(cid_list):
            cid = cid_list[i]
            if closed_retry.get(cid, 0) >= MAX_RETRY_ON_CLOSED:
                with open(unfin_file, "a") as f:
                    f.write(f"{cid}\n")
                i += 1
                continue

            try:
                page.goto(f"https://www.eeo.cn/s/a/?cid={cid}", timeout=WAIT_TIMEOUT*1000, wait_until="domcontentloaded")
                body = page.text_content("body") or ""
                if len(body.strip()) < 50:
                    school, class_name = "无", "无"
                else:
                    try:
                        page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT*1000)
                    except:
                        pass
                    # 班级名
                    class_name = "无"
                    elem = page.query_selector("p.courseName")
                    if elem:
                        text = elem.inner_text().strip()
                        if text and len(text) >= 2:
                            class_name = text
                    if class_name == "无":
                        title = page.title()
                        if "|" in title and "Join the class" not in title:
                            parts = title.split("|")
                            if len(parts) > 1:
                                class_name = parts[-1].strip()
                    # 学校名
                    school = "无"
                    elem = page.query_selector("p.schoolName")
                    if elem:
                        text = elem.inner_text().strip()
                        if text and len(text) >= 2:
                            school = text

                if not (class_name == "无" and school == "无"):
                    line = f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school} {class_name}\n"
                    add_line(line)

                if cid in closed_retry:
                    del closed_retry[cid]
                i += 1

            except Exception as e:
                err_msg = str(e).lower()
                is_closed = any(phrase in err_msg for phrase in ["closed", "context", "browser"])
                if is_closed:
                    closed_retry[cid] = closed_retry.get(cid, 0) + 1
                    try:
                        context.close()
                        page.close()
                    except:
                        pass
                    context = browser.new_context(
                        user_agent=random.choice(user_agents),
                        proxy={"server": random.choice(proxy_list)} if proxy_list else None,
                        ignore_https_errors=True
                    )
                    page = context.new_page()
                else:
                    with open(unfin_file, "a") as f:
                        f.write(f"{cid}\n")
                    i += 1
            time.sleep(SLEEP_BETWEEN)

        flush()
        browser.close()
    return worker_id

# ---------- 主函数 ----------
def main():
    global START_CID, END_CID, CID_LIST_FILE, OUTPUT_FILE, SHARD_IDX, UNFINISHED_FLAG

    if CID_LIST_FILE:
        with open(CID_LIST_FILE, "r") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
        total = len(cid_list)
        print(f"列表模式: 从 {CID_LIST_FILE} 读取 {total} 个班级")
    else:
        if START_CID is None or END_CID is None:
            print("错误: 必须指定 start_cid/end_cid 或 cid_list_file", flush=True)
            sys.exit(1)
        if START_CID > END_CID:
            print(f"警告: start_cid({START_CID}) > end_cid({END_CID})，自动交换", flush=True)
            START_CID, END_CID = END_CID, START_CID
        cid_list = list(range(START_CID, END_CID + 1))
        total = len(cid_list)
        print(f"区间模式: {START_CID} ~ {END_CID} (共 {total} 个)")
        if total == 0:
            print("错误: 没有需要扫描的班级", flush=True)
            sys.exit(1)

    print(f"Worker 并发数: {MAX_CONCURRENT}")
    print(f"批量写入大小: {BATCH_SIZE}")
    print(f"软超时限制: {TIMEOUT_HOURS} 小时 ({TIMEOUT_SECONDS} 秒)")
    print(f"强制退出等待: {FORCE_EXIT_WAIT} 秒")
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

    pool = mp.Pool(processes=worker_count)
    async_results = []
    for i, chunk in enumerate(chunks):
        res = pool.apply_async(worker_sync, (i, chunk, config_dict, PROXY_LIST, USER_AGENTS))
        async_results.append(res)

    start_time = time.time()
    soft_timeout = TIMEOUT_SECONDS
    all_done = False
    while time.time() - start_time < soft_timeout:
        if all(res.ready() for res in async_results):
            all_done = True
            break
        time.sleep(1)

    if not all_done:
        print(f"软超时已达 {soft_timeout} 秒，强制终止所有 Worker 进程...", flush=True)
        pool.terminate()
        pool.join()
        hard_timeout_start = time.time()
        while time.time() - hard_timeout_start < FORCE_EXIT_WAIT:
            time.sleep(1)
        if not all(res.ready() for res in async_results):
            print("硬超时，强制 kill", flush=True)
            pool.terminate()
            pool.join()
    else:
        pool.close()
        pool.join()

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
                    line = line.strip()
                    if line and line.isdigit():
                        unfinished_cids.add(int(line))
            os.remove(ufile)

    if unfinished_cids:
        with open(f"unfinished_cids_{SHARD_IDX}.txt", "w") as f:
            for cid in sorted(unfinished_cids):
                f.write(f"{cid}\n")
        print(f"记录了 {len(unfinished_cids)} 个未完成的 CID", flush=True)
    else:
        print("所有CID已成功处理", flush=True)

    elapsed = time.time() - start_time
    print(f"扫描结束，总耗时: {format_time(elapsed)}，结果保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    mp.freeze_support()
    main()
