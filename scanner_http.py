#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import asyncio
import traceback
import multiprocessing as mp
import psutil
import aiohttp
import re

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

# 🚀 降维打击：没有了浏览器的拖累，并发可以直接拉到 150-200！
MAX_CONCURRENT = config.get("max_concurrent_pages", 150) 
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive"
}

def format_time(seconds):
    if seconds < 0: return "0s"
    if seconds < 60: return f"{int(seconds)}s"
    m, s = divmod(int(seconds), 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

# 🚀 纯正则极速提取引擎：不建 DOM 树，0 消耗，微秒级处理！
def parse_raw_html(html):
    # 1. WAF 防火墙拦截
    if re.search(r'just a moment|cloudflare|access denied|403 forbidden|拦截|验证码', html, re.I):
        return {"status": "waf"}

    # 2. 无效页面秒退
    if re.search(r'解散|不能加入|人数已达上限|已被删除|设置了权限|不存在|页面错误|dismissed', html):
        return {"status": "not_found"}

    class_name, school, teacher = "无", "无", "无"
    found = False

    # 3. 提取 Title
    title_str = ""
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.S)
    if title_match:
        title_str = title_match.group(1).strip()

    # 4. 提取班级名
    c_match = re.search(r'class="[^"]*(?:courseName|course-title|class-name)[^"]*"[^>]*>\s*([^<]+)\s*<', html, re.I)
    if c_match:
        class_name = c_match.group(1).strip()
        found = True

    if not found and title_str and "Join" not in title_str and "eeo.cn" not in title_str:
        clean_title = title_str.replace("- ClassIn", "").replace("-ClassIn", "").replace("ClassIn", "").strip()
        if "|" in clean_title: class_name = clean_title.split("|")[-1].strip()
        elif "-" in clean_title: class_name = clean_title.split("-")[0].strip()
        else: class_name = clean_title
        if class_name: found = True

    # 5. 提取老师和学校
    if found:
        s_match = re.search(r'class="[^"]*(?:schoolName|orgName)[^"]*"[^>]*>\s*([^<]+)\s*<', html, re.I)
        if s_match: school = s_match.group(1).strip()

        t_match = re.search(r'class="[^"]*(?:teacherName|teaName|userName|courseTeacher)[^"]*"[^>]*>\s*([^<]+)\s*<', html, re.I)
        if t_match:
            teacher = t_match.group(1).strip()
        else:
            t2_match = re.search(r'教师[：:]\s*([^\s<]{1,30})', html)
            if t2_match: teacher = t2_match.group(1).strip()

        teacher = teacher.replace("授课教师：", "").replace("教师：", "").strip()
        return {"status": "success", "class_name": class_name, "school": school, "teacher": teacher}

    return {"status": "not_found"}

async def async_process_worker(process_id, cid_chunk, concurrency, deadline, shared_counter):
    in_flight_cids = set()
    results = []
    local_queue = asyncio.Queue()
    
    for cid in cid_chunk:
        local_queue.put_nowait(cid)
        
    # 🚀 连接池优化：持久化 TCP 连接，省去每次握手的开销
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    timeout = aiohttp.ClientTimeout(total=6) # 网络请求总限时 6 秒
    
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS, timeout=timeout) as session:
        
        async def fetcher(coro_id):
            while True:
                if time.time() > deadline:
                    break
                    
                try:
                    cid = local_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                in_flight_cids.add(cid)
                consecutive_errors = 0
                
                try:
                    url = f"https://www.eeo.cn/s/a/?cid={cid}"
                    async with session.get(url, allow_redirects=True) as response:
                        if response.status in [403, 429, 503]:
                            print(f"⚠️ [风控侦测] P{process_id}-C{coro_id} 遭遇HTTP状态异常 {response.status}...", flush=True)
                            raise Exception("WAF_BLOCKED")
                            
                        # 获取原始 HTML
                        html_text = await response.text(encoding='utf-8', errors='ignore')
                        
                        data = parse_raw_html(html_text)
                        status = data.get('status')
                        
                        if status == 'waf':
                            if random.random() < 0.1: 
                                print(f"⚠️ [风控验证] 页面内容提示拦截，准备重试...", flush=True)
                            raise Exception("WAF_BLOCKED")
                            
                        elif status == 'success':
                            class_name = data.get('class_name', '无')
                            school = data.get('school', '无')
                            teacher = data.get('teacher', '无')
                            invalid_marks = {"无", "-", "--", "---", "—", "_", ""}
                            if not (class_name in invalid_marks and school in invalid_marks):
                                line = f"{cid}\thttps://www.eeo.cn/s/a/?cid={cid}\t{school}\t{teacher}\t{class_name}\n"
                                results.append(line)
                                print(f"✅ [纯净捕获] P{process_id}-C{coro_id:03d} | {cid} | 🏫 {school} | 🧑‍🏫 {teacher} | 🎓 {class_name}", flush=True)

                except Exception as e:
                    consecutive_errors += 1
                    if "WAF" in str(e):
                        await asyncio.sleep(2) # 遇到拦截稍微歇一下
                
                finally:
                    if cid in in_flight_cids:
                        in_flight_cids.remove(cid)
                    local_queue.task_done()
                    
                    with shared_counter.get_lock():
                        shared_counter.value += 1

                if len(results) >= 100:
                    with open(f"data/proc_{process_id}_temp.txt", "a", encoding="utf-8") as f:
                        f.writelines(results)
                    results.clear()

        # 启动高并发的无浏览器协程群
        tasks = [asyncio.create_task(fetcher(i)) for i in range(concurrency)]
        
        wait_task = asyncio.create_task(local_queue.join())
        try:
            timeout_wait = deadline - time.time()
            if timeout_wait > 0: await asyncio.wait_for(wait_task, timeout=timeout_wait)
        except asyncio.TimeoutError:
            pass
            
        for t in tasks: t.cancel()

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

    print(f"🚀 [纯HTTP/API降维打击引擎] 启动！彻底抛弃浏览器渲染！", flush=True)
    print(f"⚙️ 分配: {process_count}个物理核心 ✕ 每核 {coros_per_process} 个网络协程 = {process_count * coros_per_process} 总并发", flush=True)

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
                print(f"\n🚨 [看门狗触发] 进度已停滞 3 分钟！启动安全收尾...", flush=True)
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
                
                print(f"\n🔥 [满血监控] 完成: {c}/{total_tasks} ({pct:.2f}%) | ⚡ 光速引擎: {speed:.1f} 个/秒 | ⏳ 剩余: {eta}")
                print(f"🖥️  [降维减负] CPU: {cpu_usage}% (彻底释放) | 💾 内存: {mem_used_gb:.1f}GB / {mem_total_gb:.1f}GB\n", flush=True)
                
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
        print(f"🚩 记录了 {len(unfinished_cids)} 个未完成 CID")
    else:
        print("✅ 所有 CID 已完美处理完毕！")

    print("🛑 引擎安全退出。", flush=True)
    os._exit(0)

if __name__ == "__main__":
    mp.set_start_method('spawn')
    main()
