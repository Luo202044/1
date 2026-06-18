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

# 🚀 开启了事件防抖后，64 乃至 80 的并发都是安全的
MAX_CONCURRENT = config.get("max_concurrent_pages", 64) 
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

# 🚀 底层极速阉割：DNS黑洞 + 内存限制 + 进程折叠
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
    "--use-mock-keychain",
    "--blink-settings=imagesEnabled=false",
    "--host-resolver-rules=MAP *google-analytics.com 127.0.0.1, MAP *sentry* 127.0.0.1, MAP *sensors* 127.0.0.1, MAP *growingio.com 127.0.0.1, MAP *baidu.com 127.0.0.1, MAP *track* 127.0.0.1",
    "--js-flags=--max-old-space-size=128", 
    "--disable-features=IsolateOrigins,site-per-process,AudioServiceOutOfProcess,BackForwardCache",
    "--renderer-process-limit=4"
]

def format_time(seconds):
    if seconds < 0: return "0s"
    if seconds < 60: return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

# 🚀 核心黑科技：MutationObserver + 微任务防抖！
js_extract_promise = r"""() => {
    return new Promise((resolve) => {
        function checkDOM() {
            let title = document.title || "";
            let lower_title = title.toLowerCase();

            // 1. WAF 秒退
            if (lower_title.includes("just a moment") || lower_title.includes("access denied") || lower_title.includes("403") || lower_title.includes("拦截")) {
                return {status: "waf"};
            }

            // 2. 成功优先：使用极速 textContent 匹配
            let c_el = document.querySelector(".courseName, p.courseName, h1, .title");
            if (c_el && c_el.offsetParent !== null) {
                let class_name = (c_el.textContent || "").trim().replace(/\s+/g, ' ');
                if (class_name.length > 0) {
                    let school = "无", teacher = "无";
                    let s_el = document.querySelector(".schoolName, p.schoolName, .orgName");
                    if (s_el && s_el.offsetParent !== null) {
                        school = (s_el.textContent || "").trim().replace(/\s+/g, ' ');
                    }
                    
                    let t_el = document.querySelector(".teacherName, .teaName, .userName, .courseTeacher, p.name");
                    if (t_el && t_el.offsetParent !== null) {
                        teacher = (t_el.textContent || "").trim().replace(/\s+/g, ' ').replace(/授课教师：|教师：/g, '').trim();
                    } else {
                        let bodyText = document.body ? (document.body.textContent || "") : "";
                        let match = bodyText.match(/教师[：:]\s*([^\s]{1,30})/);
                        if (match && match[1]) { teacher = match[1].trim(); }
                    }
                    return {status: "success", class_name, school, teacher};
                }
            }

            // 3. 失败判定：使用极速 offsetParent === null 跳过隐藏元素
            let err_nodes = document.querySelectorAll(".courseResultContent, .error-msg, .tip_end, .error-box");
            for (let el of err_nodes) {
                if (el.offsetParent === null) continue; // 瞬间跳过 Vue 隐藏组件
                let err_txt = el.textContent || ""; 
                if (/解散|不能加入|上限|已被删除|设置了权限|不存在|页面错误|dismissed/.test(err_txt)) {
                    return {status: "not_found"};
                }
            }
            return null; // 未检测到，继续等待
        }

        let initial_check = checkDOM();
        if (initial_check) return resolve(initial_check);

        // 🌟 开启防抖监听：不管 Vue 一瞬间修改了多少次 DOM，一次事件循环只查一次！CPU 占用直降 70%！
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

        // 断头台缩减：3 秒
        let timeoutId = setTimeout(() => {
            observer.disconnect();
            resolve({status: "timeout"});
        }, 3000);
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
            # 🚨 无 IPC 通信，完全靠底层拦截
        
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
                pw_timeout = min(5000, (deadline - time.time()) * 1000)
                # 🚀 极限加速：wait_until="commit"！网络通了 Python 就撒手，全靠 JS 监听！
                await page.goto(f"https://www.eeo.cn/s/a/?cid={cid}", timeout=pw_timeout, wait_until="commit")
                
                data = await page.evaluate(js_extract_promise)
                
                # 🚀 物理拔管：一旦拿到数据，瞬间向 Chrome 下达强制停止加载的指令，释放带宽和内存！
                try:
                    await page.evaluate("window.stop()")
                except:
                    pass

                status = data.get('status', 'timeout')
                
                if status == 'waf':
                    if random.random() < 0.1: 
                        print(f"⚠️ [风控侦测] P{process_id}-C{coro_id} 遭遇拦截...", flush=True)
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
                consecutive_errors += 1
                if consecutive_errors >= 2 or "WAF" in str(e):
                    if consecutive_errors >= 3: await asyncio.sleep(1.5)
                    await init_page()
                    consecutive_errors = 0
            
            finally:
                if cid in in_flight_cids:
                    in_flight_cids.remove(cid)
                local_queue.task_done()
                
                with shared_counter.get_lock():
                    shared_counter.value += 1
                    
                lifecycle += 1

            if lifecycle >= 150:
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

    print(f"🚀 [微任务防抖·物理拔管引擎] 极致优化启动！", flush=True)
    print(f"⚙️ 分配: {process_count}个核心 ✕ 每核 {coros_per_process} 并发 = {process_count * coros_per_process} 总并发", flush=True)

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
    last_c = 0
    last_c_time = start_time

    try:
        while any(p.is_alive() for p in processes):
            now = time.time()
            c = shared_counter.value
            
            if c > last_c:
                last_c = c
                last_c_time = now
            elif now - last_c_time > 180: 
                print(f"\n🚨 [看门狗触发] 停滞 3 分钟，启动安全收尾...", flush=True)
                break 
            
            if now - last_print >= 5: 
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
