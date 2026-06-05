#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import sys
import random
import multiprocessing as mp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ========== 配置加载（保持不变） ==========
# ... （省略之前的配置加载代码，与最终版相同）

# ---------- Worker 函数（同步风格，避免 asyncio 嵌套问题） ----------
def worker_sync(worker_id, cid_list, config_dict, proxy_list, user_agents):
    """同步版本的 Worker，在独立进程中运行，不依赖主进程事件循环"""
    WAIT_TIMEOUT = config_dict.get("wait_timeout", 25)
    RENDER_WAIT = config_dict.get("render_wait", 2.0)
    SLEEP_BETWEEN = config_dict.get("sleep_between", 0.3)
    RETRY_TIMES = config_dict.get("retry_times", 2)
    RETRY_DELAY = config_dict.get("retry_delay", 1.0)
    MAX_RETRY_ON_CLOSED = config_dict.get("max_retry_on_closed", 3)
    BATCH_SIZE = config_dict.get("batch_size", 200)

    # 使用同步 Playwright API
    from playwright.sync_api import sync_playwright

    completed_cids = set()
    worker_out_file = f"data/worker_{worker_id}_temp.txt"
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
                with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
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

                completed_cids.add(cid)
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
                    with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                        f.write(f"{cid}\n")
                    i += 1
            time.sleep(SLEEP_BETWEEN)

        flush()
        browser.close()

    return worker_id

# ---------- 主函数（使用 multiprocessing.Pool 并支持超时终止） ----------
async def main_async():
    global start_time, global_total, START_CID, END_CID, CID_LIST_FILE
    start_time = time.time()

    # 加载 CID 列表（与原逻辑相同）
    # ...（省略）

    print(f"Worker 并发数: {MAX_CONCURRENT}")
    print(f"软超时限制: {TIMEOUT_HOURS} 小时 ({TIMEOUT_SECONDS} 秒)")
    print(f"强制退出等待: {FORCE_EXIT_WAIT} 秒")
    print(f"Worker进程超时: {WORKER_PROCESS_TIMEOUT} 秒")

    with open(OUTPUT_FILE, "w") as f:
        f.write("")

    config_dict = {...}  # 与之前相同

    worker_count = min(MAX_CONCURRENT, len(cid_list))
    chunk_size = (len(cid_list) + worker_count - 1) // worker_count
    chunks = [cid_list[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count)]

    # 使用 multiprocessing.Pool 以便能够 terminate
    pool = mp.Pool(processes=worker_count)
    async_results = []
    for i, chunk in enumerate(chunks):
        res = pool.apply_async(worker_sync, (i, chunk, config_dict, PROXY_LIST, USER_AGENTS))
        async_results.append(res)

    # 软超时：等待 TIMEOUT_SECONDS 秒后，若未完成则终止所有进程
    soft_timeout = TIMEOUT_SECONDS
    start_wait = time.time()
    all_done = False
    while time.time() - start_wait < soft_timeout:
        if all(res.ready() for res in async_results):
            all_done = True
            break
        await asyncio.sleep(1)

    if not all_done:
        print(f"软超时已达 {soft_timeout} 秒，强制终止所有 Worker 进程...", flush=True)
        pool.terminate()
        pool.join()
        # 额外等待硬超时
        hard_timeout_start = time.time()
        while time.time() - hard_timeout_start < FORCE_EXIT_WAIT:
            await asyncio.sleep(1)
        # 确保所有进程已清理
        if not all(res.ready() for res in async_results):
            print("硬超时，强制 kill", flush=True)
            pool.terminate()
            pool.join()
    else:
        pool.close()
        pool.join()

    # 后续合并结果、收集未完成 CID 与原逻辑相同
    # ...（省略）
