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

# 🚀 突破极限：由于更换了零开销的 JS 注入引擎，并发可以直接拉高到 80！
MAX_CONCURRENT = config.get("max_concurrent_pages", 80)
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

def format_time(seconds):
    if seconds < 0: return "0s"
    if seconds < 60: return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

def init_globals(counter):
    global shared_counter
    shared_counter = counter

async def abort_route(route):
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# ========== 核心黑科技：浏览器端原生 JS 提取引擎 ==========
# 这段 JS 会在浏览器底层瞬间执行完毕，将 15 次通信压缩为 1 次，零 CPU 损耗！
js_extract = r"""() => {
    let text = document.body ? (document.body.innerText || "") : "";
    let title = document.title || "";
    let lower_title = title.toLowerCase();
    let lower_text = text.toLowerCase();

    // 风控侦测
    let waf_keywords = ["just a moment", "access denied", "attention required", "security", "403", "404", "拦截", "验证码", "error", "cloudflare", "verify you are human", "滑动验证"];
    let is_waf = false;
    for (let k of waf_keywords) {
        if (lower_title.includes(k) || lower_text.includes(k)) {
            is_waf = true; break;
        }
    }

    let class_name = "无";
    let school = "无";
    let teacher = "无";

    if (text.trim().length < 20 && !is_waf) {
        return {is_waf: false, class_name, school, teacher, title};
    }

    // 抓取班级
    let c_selectors = ["p.courseName", ".courseName", "h1", "h2", "h3", ".title", ".course-title", ".class-name"];
    for(let s of c_selectors){
        let el = document.querySelector(s);
        if(el && el.innerText.trim().length >= 1){
            class_name = el.innerText.trim().replace(/\s+/g, ' ');
            break;
        }
    }

    // 抓取学校
    let s_selectors = ["p.schoolName", ".schoolName", ".orgName"];
    for(let s of s_selectors){
        let el = document.querySelector(s);
        if(el && el.innerText.trim().length >= 1){
            school = el.innerText.trim().replace(/\s+/g, ' ');
            break;
        }
    }

    // 抓取教师
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

    // 教师跨行智能兜底
    let invalid_marks = ["无", "-", "--", "---", "—", "_", ""];
    let needs_fallback = invalid_marks.includes(teacher) || teacher === "教师" || teacher.includes("教师:") || teacher.includes("教师：");

    if (needs_fallback) {
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

    return {is_waf, class_name, school, teacher, title};
}"""

# ========== 单个进程内的异步爬虫逻辑 ==========
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
                
                # 缩短等待僵死时间：如果有效，1.2秒足够 Vue 渲染完毕。超时直接判定无效，不再傻等 2 秒
                try: await page.wait_for_selector(".courseName, .schoolName, .courseTeacher, h1", timeout=1200)
                except: pass

                # 🚀 瞬间提速点：一键注入，1毫秒内拿到所有结果
                data = await page.evaluate(js_extract)
                
                if data.get('is_waf'):
                    if random.random() < 0.1: 
                        print(f"⚠️ [风控侦测] P{process_id}-C{coro_id} 遭遇拦截，重置...", flush=True)
                    raise Exception("WAF_BLOCKED")

                class_name = data.get('class_name', '无')
                school = data.get('school', '无')
                teacher = data.get('teacher', '无')
                title = data.get('title', '')

                invalid_marks = {"无", "-", "--", "---", "—", "_", ""}

                # 标题兜底逻辑
                if class_name in invalid_marks:
                    if title and "Join the class" not in title and "eeo.cn" not in title:
                        clean_title = title.replace("- ClassIn", "").replace("-ClassIn", "").replace("ClassIn", "").strip()
                        if "|" in clean_title: 
                            class_name = clean_title.split("|")[-1].strip()
                        elif "-" in clean_title: 
                            class_name = clean_title.split("-")[0].strip()
                        elif clean_title: 
                            class_name = clean_title

                if not (class_name in invalid_marks and school in invalid_marks):
                    line = f"{cid}\thttps://www.eeo.cn/s/a/?cid={cid}\t{school}\t{teacher}\t{class_name}\n"
                    results.append(line)
                    print(f"✅ [发现] P{process_id}-C{coro_id:02d} | {cid} | 🏫 {school} | 🧑‍🏫 {teacher} | 🎓 {class_name}", flush=True)

                consecutive_errors = 0

            except Exception as e:
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
        browser = await p.chromium.launch(headless=True, args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"])
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

    print(f"🚀 [JS注入·极致引擎] 启动！", flush=True)
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
