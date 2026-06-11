#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import random
import asyncio
import traceback
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

# 极限加速：异步架构下，并发数可以直接拉高到 30
MAX_CONCURRENT = config.get("max_concurrent_pages", 30)
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
UNFINISHED_FILE = f"unfinished_cids_{SHARD_IDX}.txt"

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

# ========== 核心异步引擎状态 ==========
class ScannerState:
    def __init__(self):
        self.completed_count = 0
        self.total_count = 0
        self.in_flight_cids = set() # 内存级掉线保护，极速追踪当前正在处理的 CID
        self.results = []
        self.lock = asyncio.Lock()
        self.start_time = time.time()
        self.deadline = self.start_time + TIMEOUT_SECONDS - 60
        self.is_shutting_down = False

state = ScannerState()

# ========== 异步资源拦截器 ==========
async def abort_route(route):
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# ========== 异步 Worker 逻辑 ==========
async def worker_task(worker_id, task_queue, context):
    global state
    
    # 每个 Worker 维护自己的 page 实例
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
    lifecycle_count = 0

    while not state.is_shutting_down:
        if time.time() > state.deadline:
            break

        try:
            cid = task_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        # 记录凭证到内存
        state.in_flight_cids.add(cid)

        try:
            # 极限提速：wait_until="commit" 只等待网络响应头，不等待整个 DOM 树解析完成！
            pw_timeout = min(WAIT_TIMEOUT * 1000, (state.deadline - time.time()) * 1000)
            await page.goto(f"https://www.eeo.cn/s/a/?cid={cid}", timeout=pw_timeout, wait_until="commit")
            
            # 手动等待核心元素出现，避免无效挂起
            try:
                await page.wait_for_selector("p.courseName, p.schoolName, body", timeout=1500)
            except: pass

            title = await page.title() or ""
            body_text = await page.text_content("body") or ""
            
            # --- 风控侦测 ---
            is_waf = False
            waf_keywords = ["just a moment", "access denied", "attention required", "security", "403", "404", "拦截", "验证码", "error", "cloudflare", "verify you are human", "滑动验证"]
            if any(k in title.lower() for k in waf_keywords) or any(k in body_text.lower() for k in ["cloudflare", "verify you are human", "滑动验证"]):
                is_waf = True
                
            if is_waf:
                if random.random() < 0.1: 
                    print(f"⚠️ [风控侦测] 协程-{worker_id} 遭遇拦截，自动重置...", flush=True)
                raise Exception("WAF_BLOCKED")

            invalid_marks = {"无", "-", "--", "---", "—", "_", ""}
            school, class_name, teacher = "无", "无", "无"

            if len(body_text.strip()) >= 50:
                # 1. 抓取班级
                for selector in ["p.courseName", ".courseName", "h1", ".title"]:
                    elem = await page.query_selector(selector)
                    if elem:
                        text = (await elem.text_content() or "").strip()
                        text = " ".join(text.split())
                        if text and len(text) >= 1:
                            class_name = text
                            break
                if class_name in invalid_marks:
                    if title and "Join the class" not in title and "eeo.cn" not in title:
                        if "|" in title: class_name = title.split("|")[-1].strip()
                        elif "-" in title: class_name = title.split("-")[0].strip()

                # 2. 抓取学校
                for selector in ["p.schoolName", ".schoolName", ".orgName"]:
                    elem = await page.query_selector(selector)
                    if elem:
                        text = (await elem.text_content() or "").strip()
                        text = " ".join(text.split())
                        if text and len(text) >= 1:
                            school = text
                            break

                # 3. 抓取教师
                for selector in [".teacherName", ".teaName", ".userName", ".nickName", ".teacher-name", "p.name"]:
                    elem = await page.query_selector(selector)
                    if elem:
                        text = (await elem.text_content() or "").strip()
                        text = " ".join(text.split())
                        if text and len(text) >= 1:
                            teacher = text
                            break
                            
                # 教师兜底解析
                if teacher in invalid_marks or teacher == "教师" or teacher == "教师：" or teacher == "教师:":
                    teacher = "无"
                    try:
                        elems = await page.query_selector_all("p, div, span, label, li")
                        for i in range(len(elems)):
                            text = (await elems[i].text_content() or "").strip()
                            text = " ".join(text.split())
                            
                            if text in ["教师：", "教师:", "授课教师：", "Teacher:"]:
                                if i + 1 < len(elems):
                                    next_text = (await elems[i+1].text_content() or "").strip()
                                    next_text = " ".join(next_text.split())
                                    if next_text and len(next_text) >= 1 and len(next_text) < 50:
                                        teacher = next_text
                                        break
                            elif "教师：" in text or "授课教师：" in text:
                                extracted = text.replace("授课教师：", "").replace("教师：", "").strip()
                                if extracted and len(extracted) >= 1 and len(extracted) < 50:
                                    teacher = extracted
                                    break
                    except: pass

            # --- TSV 数据打包 ---
            if not (class_name in invalid_marks and school in invalid_marks):
                line = f"{cid}\thttps://www.eeo.cn/s/a/?cid={cid}\t{school}\t{teacher}\t{class_name}\n"
                async with state.lock:
                    state.results.append(line)
                print(f"✅ [发现] C-{worker_id:02d} | {cid} | 🏫 {school} | 🧑‍🏫 {teacher} | 🎓 {class_name}", flush=True)

            # 成功完成，从内存队列移除
            state.in_flight_cids.remove(cid)
            task_queue.task_done()
            
            async with state.lock:
                state.completed_count += 1
                
            consecutive_errors = 0
            lifecycle_count += 1

        except Exception as e:
            # 失败的记录依然标记完成，避免队列卡死，但将其保留在 in_flight_cids 中用于最后生成未完成名单
            task_queue.task_done()
            consecutive_errors += 1
            
            async with state.lock:
                state.completed_count += 1

            if consecutive_errors >= 2 or "WAF" in str(e):
                if consecutive_errors >= 3:
                    await asyncio.sleep(2)
                await init_page() # 极速重建被污染的上下文
                consecutive_errors = 0
                
            lifecycle_count += 1

        # 内存泄漏防线：定期转生
        if lifecycle_count >= 150:
            await init_page()
            lifecycle_count = 0

    if page:
        try: await page.close()
        except: pass

# ========== 异步状态汇报 ==========
async def monitor_task():
    global state
    last_print = time.time()
    while not state.is_shutting_down:
        await asyncio.sleep(1)
        now = time.time()
        if now - last_print >= 10: # 提高播报频率到 10 秒，让你直观感受速度！
            async with state.lock:
                c = state.completed_count
                total = state.total_count
                
            elapsed = now - state.start_time
            speed = c / elapsed if elapsed > 0 else 0
            rem = total - c
            eta_str = format_time(rem / speed if speed > 0 else 0)
            pct = (c / total) * 100 if total > 0 else 0
            
            print(f"\n📊 [异步引擎] 完成: {c}/{total} ({pct:.2f}%) | ⚡ 极限速度: {speed:.1f} 个/秒 | ⏳ 剩余: {eta_str}\n", flush=True)
            last_print = now
            
            # 定期持久化写入，防中途崩溃
            async with state.lock:
                if state.results:
                    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                        f.writelines(state.results)
                    state.results.clear()

# ========== 异步主函数 ==========
async def run_scanner(cid_list):
    global state
    state.total_count = len(cid_list)
    task_queue = asyncio.Queue()
    
    print(f"⚡ 正在启动全异步高速协程引擎，装载 {state.total_count} 个任务...", flush=True)
    for cid in cid_list:
        task_queue.put_nowait(cid)

    # 启动 Playwright 异步环境
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"])
        # 创建单个全局高并发 Context
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            ignore_https_errors=True
        )

        # 启动 30 个并发协程 worker
        worker_count = min(MAX_CONCURRENT, state.total_count)
        workers = [asyncio.create_task(worker_task(i, task_queue, context)) for i in range(worker_count)]
        monitor = asyncio.create_task(monitor_task())

        # 等待队列清空或超时
        wait_task = asyncio.create_task(task_queue.join())
        timeout_wait = state.deadline - time.time()
        
        try:
            if timeout_wait > 0:
                await asyncio.wait_for(wait_task, timeout=timeout_wait)
        except asyncio.TimeoutError:
            print("\n⏰ 触发安全软超时，准备下班...", flush=True)

        # 发送关闭信号
        state.is_shutting_down = True
        
        # 强制取消残留 worker
        for w in workers: w.cancel()
        monitor.cancel()
        
        await context.close()
        await browser.close()

# ========== 入口逻辑与善后扫尾 ==========
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

    try:
        # 启动核心异步事件循环
        asyncio.run(run_scanner(cid_list))
    except KeyboardInterrupt:
        print("\n⚠️ 收到强制中断信号！")
    finally:
        # === 终极扫尾：写入遗留数据与未完成凭证 ===
        print("💾 正在执行数据持久化与未完成遗嘱收集...")
        
        # 把内存里没来得及写的成功数据写盘
        if state.results:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.writelines(state.results)
                
        # 去重清理输出文件
        if os.path.exists(OUTPUT_FILE):
            seen = set()
            valid_lines = []
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if parts and parts[0] not in seen:
                        seen.add(parts[0])
                        valid_lines.append(line)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                f.writelines(valid_lines)

        # 搜刮内存里的在途 CID（因断电或软超时被遗留的）
        unfinished_cids = set(state.in_flight_cids)
        
        if unfinished_cids:
            with open(UNFINISHED_FILE, "w", encoding="utf-8") as f:
                for cid in sorted(unfinished_cids):
                    f.write(f"{cid}\n")
            open(UNFINISHED_FLAG, "w").close() 
            print(f"🚩 记录了 {len(unfinished_cids)} 个未完成的 CID，已生成补扫信标 ({UNFINISHED_FLAG})", flush=True)
        else:
            print("✅ 所有 CID 已完美处理完毕！", flush=True)

        print("🛑 引擎安全退出，释放所有底层资源。", flush=True)
        sys.stdout.flush()
        os._exit(0)

if __name__ == "__main__":
    main()
