#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
独立子进程 Worker，同步 Playwright，无 asyncio。
接收参数：worker_id, cid_list_json, config_json, proxy_list_json, user_agents_json, output_dir
"""

import sys
import json
import time
import random
import os
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

def run_worker(worker_id, cid_list, config, proxy_list, user_agents, output_dir):
    WAIT_TIMEOUT = config.get("wait_timeout", 25)
    RENDER_WAIT = config.get("render_wait", 2.0)
    SLEEP_BETWEEN = config.get("sleep_between", 0.3)
    RETRY_TIMES = config.get("retry_times", 2)
    RETRY_DELAY = config.get("retry_delay", 1.0)
    MAX_RETRY_ON_CLOSED = config.get("max_retry_on_closed", 3)
    BATCH_SIZE = config.get("batch_size", 200)

    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, f"output_{worker_id}.txt")
    unfin_file = f"unfinished_worker_{worker_id}.txt"

    completed = set()
    write_buffer = []

    def flush():
        if write_buffer:
            with open(out_file, "a", encoding="utf-8") as f:
                f.write("".join(write_buffer))
            write_buffer.clear()

    def add_line(line):
        write_buffer.append(line)
        if len(write_buffer) >= BATCH_SIZE:
            flush()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
                "--disable-setuid-sandbox", "--disable-accelerated-2d-canvas"
            ]
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
                    school_name, class_name = "无", "无"
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
                    school_name = "无"
                    elem = page.query_selector("p.schoolName")
                    if elem:
                        text = elem.inner_text().strip()
                        if text and len(text) >= 2:
                            school_name = text

                if not (class_name == "无" and school_name == "无"):
                    line = f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school_name} {class_name}\n"
                    add_line(line)

                completed.add(cid)
                if cid in closed_retry:
                    del closed_retry[cid]
                i += 1

            except (PlaywrightTimeoutError, Exception) as e:
                err_msg = str(e).lower()
                if any(phrase in err_msg for phrase in ["closed", "context", "browser"]):
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

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: worker_sync.py worker_id cid_list_json config_json proxy_list_json user_agents_json output_dir", file=sys.stderr)
        sys.exit(1)
    worker_id = int(sys.argv[1])
    cid_list = json.loads(sys.argv[2])
    config = json.loads(sys.argv[3])
    proxy_list = json.loads(sys.argv[4])
    user_agents = json.loads(sys.argv[5])
    output_dir = sys.argv[6]
    run_worker(worker_id, cid_list, config, proxy_list, user_agents, output_dir)
