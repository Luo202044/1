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
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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

# 🚀 因为消除了标签页重建的 CPU 损耗，并发可以安全地拉回到 48 或 60！
MAX_CONCURRENT = config.get("max_concurrent_pages", 48) 
WAIT_TIMEOUT = config.get("wait_timeout", 15)
TIMEOUT_HOURS = config.get("timeout_hours", 5.0)
TIMEOUT_SECONDS = TIMEOUT_HOURS * 3600

os.makedirs("data", exist_ok=True)

if START_CID is not None and END_CID is not None and not CID_LIST_FILE:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "") if CID_LIST_FILE else "unknown"
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

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
    "--disable-features=AudioServiceOutOfProcess",
    "--disable-hang-monitor",
    "--disable-ipc-flooding-protection",
    "--disable-notifications",
    "--disable-offer-store-unmasked-wallet-cards",
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
    "--use-mock-keychain"
]

def format_time(seconds):
    if seconds < 0: return "0s"
    if seconds < 60: return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

async def abort_route(route):
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# ========== 核心黑科技：JS 内部轮询，杜绝 Python 异常阻塞 ==========
js_extract_promise = r"""() => {
    return new Promise((resolve) => {
        let elapsed = 0;
        let interval = setInterval(() => {
            let text = document.body ? (document.body.innerText || "") : "";
            let title = document.title || "";
            let lower_title = title.toLowerCase();
            let lower_text = text.toLowerCase();

            // 1. 深度风控拦截侦测 (秒级中止)
            let waf_keywords = ["just a moment", "access denied", "attention required", "security", "403", "404", "拦截", "验证码", "error", "cloudflare", "verify you are human", "滑动验证"];
            for (let k of waf_keywords) {
                if (lower_title.includes(k) || lower_text.includes(k)) {
                    clearInterval(interval);
                    resolve({status: "waf"}); return;
                }
            }

            // 2. 无效页面秒退机制 (发现已知错误关键字，0.1秒内立刻退出，绝不傻等1.2秒)
            let failure_keywords = ["班级解散", "不能加入", "人数已达上限", "已被删除", "设置了权限", "不存在", "页面错误"];
            for (let k of failure_keywords) {
                 if (text.includes(k)) {
                     clearInterval(interval);
                     resolve({status: "not_found"}); return;
                 }
            }

            let class_name = "无", school = "无", teacher = "无";
            let found = false;

            // 3. 寻找成功标识
            let c_selectors = ["p.courseName", ".courseName", "h1", "h2", "h3", ".title", ".course-title", ".class-name"];
            for(let s of c_selectors){
                let el = document.querySelector(s);
                if(el && el.innerText.trim().length >= 1){
                    class_name = el.innerText.trim().replace(/\s+/g, ' ');
                    found = true; break;
                }
            }

            if (!found && title && !title.includes("Join the class") && !title.includes("eeo.cn")) {
                let clean_title = title.replace("- ClassIn", "").replace("-ClassIn", "").replace("ClassIn", "").trim();
                if (clean_title.includes("|")) { class_name = clean_title.split("|").pop().trim(); if(class_name) found = true; } 
                else if (clean_title.includes("-")) { class_name = clean_title.split("-")[0].trim(); if(class_name) found = true; } 
                else if (clean_title) { class_name = clean_title; found = true; }
            }

            // 4. 一旦发现班级，提取全部信息并极速返回
            if (found) {
                let s_selectors = ["p.schoolName", ".schoolName", ".orgName"];
                for(let s of s_selectors){
                    let el = document.querySelector(s);
                    if(el && el.innerText.trim().length >= 1){ school = el.innerText.trim().replace(/\s+/g, ' '); break; }
                }

                let t_selectors = [".teacherName", ".teaName", ".userName", ".nickName", ".teacher-name", "p.name", ".courseTeacher"];
                for(let s of t_selectors){
                    let el = document.querySelector(s);
                    if(el && el.innerText.trim().length >= 1){
                        teacher = el.innerText.trim().replace(/\s+/g, ' ');
                        if(teacher.includes("教师：") || teacher.includes("授课教师：")){
                            teacher = teacher.replace("授课教师：", "").replace("教师：", "").trim();
                        }
                        break;
                    }
                }

                let invalid_marks = ["无", "-", "--", "---", "—", "_", ""];
                if (invalid_marks.includes(teacher) || teacher === "教师" || teacher.includes("教师:") || teacher.includes("教师：")) {
                     teacher = "无";
                     let els = document.querySelectorAll("p, div, span, label, li");
                     for (let i=0; i<els.length; i++) {
                         let txt = (els[i].innerText || els[i].textContent || "").trim().replace(/\s+/g, " ");
                         if (txt === "教师：" || txt === "教师:" || txt === "授课教师：" || txt === "Teacher:") {
                             if (i + 1 < els.length) {
                                 let n_text = (els[i+1].innerText || els[i+1].textContent || "").trim().replace(/\s+/g, " ");
                                 if (n_text && n_text.length >= 1 && n_text.length < 50) { teacher = n_text; break; }
                             }
                         } else if (txt.includes("教师：") || txt.includes("授课教师：")) {
                             let ext = txt.replace("授课教师：", "").replace("教师：", "").trim();
                             if (ext && ext.length >= 1 && ext.length < 50) { teacher = ext; break; }
                         }
                     }
                }

                clearInterval(interval);
                resolve({status: "success", class_name, school, teacher}); return;
            }

            // 5. 超时判定 (达到 1.2 秒没有任何有效内容，视为无效页面)
            elapsed += 50;
            if (elapsed >= 1200) {
                clearInterval(interval);
                resolve({status: "timeout"});
            }
        }, 50);
    });
}"""

async def async_process_worker(process_id, cid_chunk, concurrency, deadline, shared_counter):
    in_flight_cids = set()
    results = []
    local_queue = asyncio.Queue()
    
    for cid in cid_chunk:
        local_queue.put_nowait(cid)
        
    async def fetcher(coro_id, context):
        page = None
        
        async def init_page():
            nonlocal page
            if page:
                try: await page.close()
                except: pass
            page = await context.new_page()
            await page.route("**/*", abort_route)
            
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
                pw_timeout = min(WAIT_TIMEOUT * 1000, (deadline - time.time()) * 1000)
                await page.goto(f"https://www.eeo.cn/s/a/?cid={cid}", timeout=pw_timeout, wait_until="domcontentloaded")
                
                # 🚀 这里移除了 wait_for_selector，直接进入 JS Promise 轮询，再也不会触发 Python 的抛错重建机制了！
                data = await page.evaluate(js_extract_promise)
                status = data.get('status', 'timeout')
                
                if status == 'waf':
                    if random.random() < 0.1: 
                        print(f"⚠️ [风控侦测] P{process_id}-C{coro_id} 遭遇拦截，重置...", flush=True)
                    raise Exception("WAF_BLOCKED")
                    
                elif status == 'success':
                    class_name = data.get('class_name', '无')
                    school = data.get('school', '无')
                    teacher = data.get('teacher', '无')
                    invalid_marks = {"无", "-", "--", "---", "—", "_", ""}
                    if not (class_name in invalid_marks and school in invalid_marks):
                        line = f"{cid}\thttps://www.eeo.cn/s/a/?cid={cid}\t{school}\t{teacher}\t{class_name}\n"
                        results.append(line)
                        print(f"✅ [发现] P{process_id}-C{coro_id:02d} | {cid} | 🏫 {school} | 🧑‍🏫 {teacher} | 🎓 {class_name}", flush=True)

                consecutive_errors = 0

            except Exception as e:
                # 只有遇到极其恶劣的真·网络断线或者 WAF 封锁时，才会进入这里的错误处理
                consecutive_errors += 1
                if consecutive_errors >= 2 or "WAF" in str(e):
                    if consecutive_errors >= 3: await asyncio.sleep(2)
                    await init_page()
                    consecutive_errors = 0
            
            finally:
                if cid in in_flight_cids:
                    in_flight_cids.remove(cid)
                local_queue.task_done()
                
                with shared_counter.get_lock():
                    shared_counter.value += 1
                    
                lifecycle += 1

            # 放宽生命周期，减少无谓的关闭开启
            if lifecycle >= 300:
                await init_page()
                lifecycle = 0

            if len(results) >= 100:
                with open(f"data/proc_{process_id}_temp.txt", "a", encoding="utf-8") as f:
                    f.writelines(results)
                results.clear()

        if page:
            try: await page.close()
            except: pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROME_OPTIMIZED_ARGS)
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS), ignore_https_errors=True)
        
        tasks = [asyncio.create_task(fetcher(i, context)) for i in range(concurrency)]
        
        wait_task = asyncio.create_task(local_queue.join())
        try:
            timeout_wait = deadline - time.time()
            if timeout_wait > 0: await asyncio.wait_for(wait_task, timeout=timeout_wait)
        except asyncio.TimeoutError:
            pass
            
        for t in tasks: t.cancel()
        await context.close()
        await browser.close()

    if results:
        with open(f"data/proc_{process_id}_temp.txt", "a", encoding="utf-8") as f:
            f.writelines(results)
            
    while not local_queue.empty():
        in_flight_cids.add(local_queue.get_nowait())
        
    if in_flight_cids:
        with open(f"unfinished_proc_{process_id}.txt", "w", encoding="utf-8") as f:
            for cid in in_flight_cids: f.write(f"{cid}\n")

def process_runner(process_id, cid_chunk, concurrency, deadline, shared_counter):
    try:
        asyncio.run(async_process_worker(process_id, cid_chunk, concurrency, deadline, shared_counter))
    except Exception as e:
        print(f"\n❌ [进程 {process_id}] 发生内部崩溃: {e}\n{traceback.format_exc()}\n", flush=True)

def main():
    global START_CID, END_CID, CID_LIST_FILE, OUTPUT_FILE

    if CID_LIST_FILE:
        with open(CID_LIST_FILE, "r", encoding="utf-8") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
    else:
        if START_CID > END_CID: START_CID, END_CID = END_CID, START_CID
        cid_list = list(range(START_CID, END_CID + 1))

    if not cid_list:
        print("错误: 任务列表为空", flush=True)
        sys.exit(1)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f: f.write("")

    total_tasks = len(cid_list)
    process_count = min(4, total_tasks) 
    coros_per_process = max(1, MAX_CONCURRENT // process_count)

    chunk_size = (total_tasks + process_count - 1) // process_count
    chunks = [cid_list[i:i + chunk_size] for i in range(0, total_tasks, chunk_size)]

    print(f"🚀 [无损轮询·极速引擎] 启动！彻底消除标签页重建设损耗！", flush=True)
    print(f"⚙️ 分配: {process_count}个物理核心 ✕ 每核 {coros_per_process} 个协程并发 = {process_count * coros_per_process} 总并发", flush=True)

    shared_counter = mp.Value('i', 0)
    deadline = time.time() + TIMEOUT_SECONDS - 60
    start_time = time.time()
    
    psutil.cpu_percent(interval=None)

    processes = []
    for i in range(len(chunks)):
        p = mp.Process(target=process_runner, args=(i, chunks[i], coros_per_process, deadline, shared_counter))
        processes.append(p)
        p.start()

    last_print = start_time
    try:
        while any(p.is_alive() for p in processes):
            now = time.time()
            if now - last_print >= 5: 
                c = shared_counter.value
                elapsed = now - start_time
                speed = c / elapsed if elapsed > 0 else 0
                rem = total_tasks - c
                eta = format_time(rem / speed if speed > 0 else 0)
                pct = (c / total_tasks) * 100 if total_tasks > 0 else 0
                
                cpu_usage = psutil.cpu_percent(interval=None)
                mem_info = psutil.virtual_memory()
                mem_used_gb = mem_info.used / (1024 ** 3)
                mem_total_gb = mem_info.total / (1024 ** 3)
                
                print(f"\n🔥 [满血监控] 完成: {c}/{total_tasks} ({pct:.2f}%) | ⚡ 飙车时速: {speed:.1f} 个/秒 | ⏳ 剩余: {eta}")
                print(f"🖥️  [硬件状态] CPU: {cpu_usage}% | 💾 内存: {mem_used_gb:.1f}GB / {mem_total_gb:.1f}GB ({mem_info.percent}%)\n", flush=True)
                
                last_print = now
                
            if now > deadline + 60:
                print("硬超时触发，强制掐断主进程...", flush=True)
                break
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ 收到强制中断信号！")
    
    for p in processes:
        p.terminate()
        p.join()

    print("💾 正在执行数据持久化与未完成收集...")
    
    seen = set()
    valid_lines = []
    for i in range(process_count):
        tmp_file = f"data/proc_{i}_temp.txt"
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
    for i in range(process_count):
        ufile = f"unfinished_proc_{i}.txt"
        if os.path.exists(ufile):
            with open(ufile, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().isdigit(): unfinished_cids.add(int(line.strip()))
            os.remove(ufile)

    if unfinished_cids:
        with open(f"unfinished_cids_{SHARD_IDX}.txt", "w", encoding="utf-8") as f:
            for cid in sorted(unfinished_cids): f.write(f"{cid}\n")
        open(UNFINISHED_FLAG, "w").close() 
        print(f"🚩 记录了 {len(unfinished_cids)} 个未完成 CID，生成补扫信标 ({UNFINISHED_FLAG})")
    else:
        print("✅ 所有 CID 已完美处理完毕！")

    print("🛑 引擎安全退出，释放所有底层资源。", flush=True)
    os._exit(0)

if __name__ == "__main__":
    mp.set_start_method('spawn')
    main()
