#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import random
import asyncio
import traceback
import multiprocessing as mp
import psutil
from playwright.async_api import async_playwright

# ========== 强制行缓冲 ==========
sys.stdout.reconfigure(line_buffering=True)

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

# 🔥 单进程并发数建议 25~30，可根据实际情况调整（通过 config.json 的 max_concurrent_pages）
MAX_CONCURRENT = min(config.get("max_concurrent_pages", 25), 40)
TIMEOUT_SECONDS = config.get("timeout_hours", 3.0) * 3600

os.makedirs("data", exist_ok=True)

if START_CID is not None and END_CID is not None and not CID_LIST_FILE:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "") if CID_LIST_FILE else "unknown"
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

def format_time(seconds):
    if seconds < 0: return "0s"
    if seconds < 60: return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Chromium 优化参数（增加内存限制）
CHROME_OPTIMIZED_ARGS = [
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-breakpad",
    "--disable-component-update",
    "--disable-default-apps",
    "--disable-domain-reliability",
    "--disable-extensions",
    "--disable-hang-monitor",
    "--disable-ipc-flooding-protection",
    "--disable-notifications",
    "--disable-popup-blocking",
    "--disable-print-preview",
    "--disable-prompt-on-repost",
    "--disable-renderer-backgrounding",
    "--disable-setuid-sandbox",
    "--disable-speech-api",
    "--disable-sync",
    "--hide-scrollbars",
    "--ignore-gpu-blacklist",
    "--metrics-recording-only",
    "--mute-audio",
    "--no-default-browser-check",
    "--no-first-run",
    "--no-pings",
    "--no-zygote",
    "--password-store=basic",
    "--use-gl=swiftshader",
    "--use-mock-keychain",
    "--blink-settings=imagesEnabled=false",
    "--host-resolver-rules=MAP *google-analytics.com 127.0.0.1, MAP *sentry* 127.0.0.1, MAP *sensors* 127.0.0.1, MAP *growingio.com 127.0.0.1, MAP *baidu.com 127.0.0.1, MAP *track* 127.0.0.1",
    "--js-flags=--max-old-space-size=512",   # 从 128 提升到 512
    "--disable-features=IsolateOrigins,site-per-process,AudioServiceOutOfProcess,BackForwardCache",
    "--renderer-process-limit=4"
]

# 提取 JS（与之前相同）
js_extract_promise = r"""() => {
    return new Promise((resolve) => {
        function checkDOM() {
            let title = document.title || "";
            let lower_title = title.toLowerCase();

            if (lower_title.includes("just a moment") || lower_title.includes("access denied") || lower_title.includes("403") || lower_title.includes("拦截")) {
                return {status: "waf"};
            }

            let class_name = "无", school = "无", teacher = "无";
            let found = false;

            let c_el = document.querySelector("p.courseName, .courseName, h1, .title");
            if (c_el && c_el.style.display !== 'none') {
                let txt = (c_el.textContent || "").trim();
                if (txt) { class_name = txt.replace(/\s+/g, ' '); found = true; }
            }

            if (!found && title && !title.includes("Join") && !title.includes("eeo.cn")) {
                let clean_title = title.replace("- ClassIn", "").trim();
                if (clean_title.includes("|")) { class_name = clean_title.split("|").pop().trim(); if(class_name) found = true; } 
                else if (clean_title.includes("-")) { class_name = clean_title.split("-")[0].trim(); if(class_name) found = true; } 
                else if (clean_title) { class_name = clean_title; found = true; }
            }

            if (found) {
                let s_el = document.querySelector("p.schoolName, .schoolName, .orgName");
                if (s_el && s_el.style.display !== 'none') { school = (s_el.textContent || "").trim().replace(/\s+/g, ' '); }

                let t_el = document.querySelector(".teacherName, .teaName, .userName, .courseTeacher, p.name");
                if (t_el && t_el.style.display !== 'none') {
                    teacher = (t_el.textContent || "").trim().replace(/\s+/g, ' ');
                    if (teacher.includes("教师：") || teacher.includes("授课教师：")) {
                        teacher = teacher.replace("授课教师：", "").replace("教师：", "").trim();
                    }
                } else {
                    let bodyText = document.body ? (document.body.textContent || "") : "";
                    let match = bodyText.match(/教师[：:]\s*([^\s]{1,30})/);
                    if (match && match[1]) { teacher = match[1].trim(); }
                }

                return {status: "success", class_name, school, teacher};
            }

            let err_nodes = document.querySelectorAll(".courseResultContent, .error-msg, .tip_end, .error-box");
            for (let i = 0; i < err_nodes.length; i++) {
                let el = err_nodes[i];
                if (el.style.display === 'none') continue; 
                let err_txt = el.textContent || "";
                if (err_txt.includes("解散") || err_txt.includes("不能加入") || err_txt.includes("上限") || err_txt.includes("已被删除") || err_txt.includes("设置了权限") || err_txt.includes("不存在") || err_txt.includes("页面错误") || err_txt.includes("dismissed")) {
                    return {status: "not_found"};
                }
            }
            return null; 
        }

        let initial_check = checkDOM();
        if (initial_check) return resolve(initial_check);

        let isChecking = false;
        let observer = new MutationObserver(() => {
            if (isChecking) return; 
            isChecking = true;
            Promise.resolve().then(() => {
                let res = checkDOM();
                if (res) {
                    observer.disconnect(); 
                    clearTimeout(timeoutId);
                    resolve(res);
                }
                isChecking = false;
            });
        });
        
        observer.observe(document, { childList: true, subtree: true, characterData: true });

        let timeoutId = setTimeout(() => {
            observer.disconnect();
            resolve({status: "timeout"});
        }, 6000);   // 从 3200 提升到 6000
    });
}"""

# ========== 单进程异步工作器 ==========
async def async_worker(cid_chunk, concurrency, deadline, shared_counter, total_tasks, start_time):
    in_flight_cids = set()
    results = []
    local_queue = asyncio.Queue()
    for cid in cid_chunk:
        local_queue.put_nowait(cid)

    last_print = time.time()
    last_count = 0
    watchdog_time = time.time()

    async def fetcher(coro_id, context):
        page = None
        async def init_page():
            nonlocal page
            if page:
                try: await page.close()
                except: pass
            page = await context.new_page()
        await init_page()
        consecutive_errors = 0
        lifecycle = 0

        while True:
            if time.time() > deadline:
                break
            try:
                cid = local_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            in_flight_cids.add(cid)
            try:
                pw_timeout = min(15000, (deadline - time.time()) * 1000)
                await page.goto(f"https://www.eeo.cn/s/a/?cid={cid}", timeout=pw_timeout, wait_until="commit")
                data = await page.evaluate(js_extract_promise)
                try:
                    await page.evaluate("window.stop()")
                except:
                    pass

                status = data.get('status', 'timeout')
                if status == 'waf':
                    if random.random() < 0.1:
                        print(f"⚠️ [风控侦测] C{coro_id:02d} 遭遇拦截", flush=True)
                    raise Exception("WAF_BLOCKED")
                elif status == 'success':
                    class_name = data.get('class_name', '无')
                    school = data.get('school', '无')
                    teacher = data.get('teacher', '无')
                    invalid_marks = {"无", "-", "--", "---", "—", "_", ""}
                    if not (class_name in invalid_marks and school in invalid_marks):
                        line = f"{cid}\thttps://www.eeo.cn/s/a/?cid={cid}\t{school}\t{teacher}\t{class_name}\n"
                        results.append(line)
                        print(f"✅ [满血解析] C{coro_id:02d} | {cid} | 🏫 {school} | 🧑‍🏫 {teacher}", flush=True)
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= 2 or "WAF" in str(e):
                    if consecutive_errors >= 3:
                        await asyncio.sleep(1.5)
                    await init_page()
                    consecutive_errors = 0
            finally:
                if cid in in_flight_cids:
                    in_flight_cids.remove(cid)
                local_queue.task_done()
                with shared_counter.get_lock():
                    shared_counter.value += 1
                lifecycle += 1

            if lifecycle >= 100:
                await init_page()
                lifecycle = 0
            if len(results) >= 100:
                with open(f"data/proc_temp.txt", "a", encoding="utf-8") as f:
                    f.writelines(results)
                results.clear()

        if page:
            try: await page.close()
            except: pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROME_OPTIMIZED_ARGS)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            ignore_https_errors=True
        )
        context.set_default_timeout(15000)

        tasks = [asyncio.create_task(fetcher(i, context)) for i in range(concurrency)]
        wait_task = asyncio.create_task(local_queue.join())

        # 进度监控（每2秒打印）
        try:
            while not local_queue.empty() or any(not t.done() for t in tasks):
                now = time.time()
                if now - last_print >= 2:
                    done = shared_counter.value
                    elapsed = now - start_time
                    speed = done / elapsed if elapsed > 0 else 0
                    rem = total_tasks - done
                    eta = format_time(rem / speed if speed > 0 else 0)
                    pct = (done / total_tasks) * 100 if total_tasks > 0 else 0
                    cpu = psutil.cpu_percent(interval=None)
                    mem = psutil.virtual_memory()
                    print(f"\n🔥 [进度] 完成: {done}/{total_tasks} ({pct:.2f}%) | ⚡ {speed:.1f} 个/秒 | ⏳ 剩余 {eta} | CPU {cpu}% | 内存 {mem.used/(1024**3):.1f}GB/{mem.total/(1024**3):.1f}GB", flush=True)
                    last_print = now

                    if done == last_count:
                        if now - watchdog_time > 60:
                            print(f"⚠️ [看门狗] 完成数 {done} 已超过 60 秒未变化", flush=True)
                            watchdog_time = now
                    else:
                        last_count = done
                        watchdog_time = now

                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

        try:
            timeout_wait = deadline - time.time()
            if timeout_wait > 0:
                await asyncio.wait_for(wait_task, timeout=timeout_wait)
        except asyncio.TimeoutError:
            print("⏱️ 总超时到达，停止接收新任务", flush=True)

        for t in tasks:
            t.cancel()
        await context.close()
        await browser.close()

    if results:
        with open(f"data/proc_temp.txt", "a", encoding="utf-8") as f:
            f.writelines(results)

    while not local_queue.empty():
        in_flight_cids.add(local_queue.get_nowait())
    if in_flight_cids:
        with open(f"unfinished_proc.txt", "w", encoding="utf-8") as f:
            for cid in in_flight_cids:
                f.write(f"{cid}\n")

def main():
    global START_CID, END_CID, CID_LIST_FILE, OUTPUT_FILE

    if CID_LIST_FILE:
        with open(CID_LIST_FILE, "r", encoding="utf-8") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
    else:
        if START_CID > END_CID:
            START_CID, END_CID = END_CID, START_CID
        cid_list = list(range(START_CID, END_CID + 1))

    if not cid_list:
        print("错误: 任务列表为空", flush=True)
        sys.exit(1)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")

    total_tasks = len(cid_list)
    concurrency = min(MAX_CONCURRENT, total_tasks)

    print("=" * 60, flush=True)
    print("🚀 [Playwright 单进程异步] 启动！", flush=True)
    if START_CID is not None and END_CID is not None:
        print(f"📋 扫描范围: {START_CID} ~ {END_CID} (共 {total_tasks} 个 CID)", flush=True)
    else:
        print(f"📋 扫描列表: {CID_LIST_FILE} (共 {total_tasks} 个 CID)", flush=True)
    print(f"⚙️  总并发: {concurrency} (可通过 config.json 调整 max_concurrent_pages)", flush=True)
    print(f"⏱️  超时: {TIMEOUT_SECONDS/3600:.1f} 小时", flush=True)
    print(f"💾 输出文件: {OUTPUT_FILE}", flush=True)
    print("=" * 60, flush=True)

    shared_counter = mp.Value('i', 0)
    deadline = time.time() + TIMEOUT_SECONDS - 60
    start_time = time.time()

    try:
        asyncio.run(async_worker(cid_list, concurrency, deadline, shared_counter, total_tasks, start_time))
    except Exception as e:
        print(f"❌ 错误: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)

    # 合并临时文件
    tmp_file = "data/proc_temp.txt"
    seen = set()
    valid_lines = []
    if os.path.exists(tmp_file):
        with open(tmp_file, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split()
                if parts and parts[0] not in seen:
                    seen.add(parts[0])
                    valid_lines.append(line)
        os.remove(tmp_file)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.writelines(valid_lines)

    unfinished_cids = set()
    ufile = "unfinished_proc.txt"
    if os.path.exists(ufile):
        with open(ufile, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().isdigit():
                    unfinished_cids.add(int(line.strip()))
        os.remove(ufile)

    if unfinished_cids:
        with open(f"unfinished_cids_{SHARD_IDX}.txt", "w", encoding="utf-8") as f:
            for cid in sorted(unfinished_cids):
                f.write(f"{cid}\n")
        open(UNFINISHED_FLAG, "w").close()
        print(f"⚠️ 未完成 {len(unfinished_cids)} 个 CID，已写入 {UNFINISHED_FLAG}", flush=True)
    else:
        print("✅ 所有 CID 已完美处理完毕！", flush=True)

    os._exit(0)

if __name__ == "__main__":
    try:
        mp.set_start_method('spawn')
    except:
        pass
    main()