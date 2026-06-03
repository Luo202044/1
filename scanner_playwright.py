#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
import os
import sys
import random
from asyncio import Lock, Semaphore
from playwright.async_api import async_playwright

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

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
MAX_CONCURRENT_PAGES = config.get("max_concurrent_pages", 10)   # 同时最多打开的页面数（池大小）
WAIT_TIMEOUT = config.get("wait_timeout", 10)                  # 基础超时（秒）
RENDER_WAIT = config.get("render_wait", 2)                     # 等待元素渲染时间（秒）
SLEEP_BETWEEN = config.get("sleep_between", 0.1)               # 每个CID处理后的最小间隔
RETRY_TIMES = config.get("retry_times", 2)
RETRY_DELAY = config.get("retry_delay", 1)
TIMEOUT_HOURS = config.get("timeout_hours", 5.5)
TIMEOUT_SECONDS = TIMEOUT_HOURS * 3600
BATCH_SIZE = config.get("batch_size", 200)
MAX_RETRY_ON_CLOSED = 3

os.makedirs("data", exist_ok=True)

# 输出文件
if START_CID is not None and END_CID is not None:
    OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")
else:
    base = os.path.basename(CID_LIST_FILE).replace(".txt", "")
    OUTPUT_FILE = os.path.join("data", f"list_{base}.txt")

SHARD_IDX = os.environ.get("SHARD_IDX", "unknown")
UNFINISHED_FLAG = f"unfinished_{SHARD_IDX}.flag"

# 全局状态
global_total = 0
global_completed = 0
global_lock = Lock()
write_buffer = []
buffer_lock = Lock()
start_time = None
stop_event = asyncio.Event()

# 代理和UA（与原一致）
PROXY_LIST = []
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ---------- 资源监控（与原一致）----------
try:
    import psutil
    PSUTIL_AVAILABLE = True
    TOTAL_MEM_GB = psutil.virtual_memory().total / (1024 ** 3)
except ImportError:
    PSUTIL_AVAILABLE = False

def get_system_memory_str():
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        mem = psutil.virtual_memory()
        used_gb = mem.used / (1024 ** 3)
        return f"{used_gb:.2f}GB/{TOTAL_MEM_GB:.1f}GB"
    except:
        return "?GB/?GB"

def get_cpu_percent_str():
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        cpu = psutil.cpu_percent(interval=0)
        return f"{cpu:.1f}"
    except:
        return "?"

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

# ---------- 批量写入（与原一致）----------
async def flush_buffer():
    global write_buffer
    if not write_buffer:
        return
    async with buffer_lock:
        if write_buffer:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write("".join(write_buffer))
            write_buffer.clear()

async def add_to_buffer(line):
    global write_buffer
    async with buffer_lock:
        write_buffer.append(line)
        if len(write_buffer) >= BATCH_SIZE:
            await flush_buffer()

# ---------- 页面提取（自适应超时）----------
async def fetch_page_data(page, cid, timeout_override=None):
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    actual_timeout = (timeout_override or WAIT_TIMEOUT) * 1000
    try:
        await page.goto(url, timeout=actual_timeout, wait_until="domcontentloaded")
        body_text = await page.text_content("body") or ""
        if len(body_text.strip()) < 50:
            return "无", "无"
        # 等待动态内容
        try:
            await page.wait_for_selector("p.courseName, p.schoolName", timeout=RENDER_WAIT * 1000)
        except:
            pass
        # 班级名称
        class_name = None
        elem = await page.query_selector("p.courseName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                class_name = text
        if not class_name:
            title = await page.title()
            if "|" in title and "Join the class" not in title:
                parts = title.split("|")
                if len(parts) > 1:
                    class_name = parts[-1].strip()
        class_str = class_name if class_name else "无"
        # 学校名称
        school_name = None
        elem = await page.query_selector("p.schoolName")
        if elem:
            text = (await elem.inner_text()).strip()
            if text and len(text) >= 2:
                school_name = text
        school_str = school_name if school_name else "无"
        return school_str, class_str
    except asyncio.TimeoutError:
        raise Exception("Timeout")
    except Exception as e:
        raise e

async def process_cid_with_retry(page, cid, retry=0, last_timeout=None):
    """自适应超时重试：每次超时后增加超时时间"""
    try:
        timeout_val = last_timeout if last_timeout else WAIT_TIMEOUT
        return await fetch_page_data(page, cid, timeout_override=timeout_val)
    except Exception as e:
        if retry < RETRY_TIMES and "Timeout" in str(e):
            # 指数增加超时时间: 10s -> 20s -> 30s
            new_timeout = WAIT_TIMEOUT * (retry + 1)
            delay = RETRY_DELAY * (2 ** retry)
            await asyncio.sleep(delay)
            return await process_cid_with_retry(page, cid, retry + 1, new_timeout)
        else:
            raise e

# ---------- 浏览器上下文池 ----------
class ContextPool:
    """管理固定数量的 Playwright Context（每个Context一个Page）"""
    def __init__(self, browser, pool_size):
        self.browser = browser
        self.pool_size = pool_size
        self.available = asyncio.Queue()
        self.all_contexts = []
        self._lock = asyncio.Lock()

    async def initialize(self):
        """预创建所有 Context 和 Page"""
        for i in range(self.pool_size):
            context = await self.browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                proxy={"server": random.choice(PROXY_LIST)} if PROXY_LIST else None,
                ignore_https_errors=True,
                java_script_enabled=True
            )
            page = await context.new_page()
            # 验证页面可用
            try:
                await page.evaluate("1+1")
            except Exception as e:
                raise Exception(f"新建 page 不可用: {e}")
            self.all_contexts.append((context, page))
            await self.available.put((context, page))

    async def acquire(self):
        """获取一个可用的 (context, page)，如果全部繁忙则等待"""
        return await self.available.get()

    async def release(self, context, page, is_broken=False):
        """归还资源，如果损坏则重建"""
        if is_broken:
            # 关闭损坏的 context
            try:
                await context.close()
            except:
                pass
            # 重建一个新的 context+page
            new_ctx = await self.browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                proxy={"server": random.choice(PROXY_LIST)} if PROXY_LIST else None,
                ignore_https_errors=True,
                java_script_enabled=True
            )
            new_page = await new_ctx.new_page()
            await new_page.evaluate("1+1")
            # 替换列表中的旧引用
            async with self._lock:
                for i, (c, p) in enumerate(self.all_contexts):
                    if c == context:
                        self.all_contexts[i] = (new_ctx, new_page)
                        break
            context, page = new_ctx, new_page
        await self.available.put((context, page))

    async def close_all(self):
        """关闭所有资源"""
        for context, page in self.all_contexts:
            try:
                await page.close()
            except:
                pass
            try:
                await context.close()
            except:
                pass

# ---------- Worker（从池中获取页面）----------
async def worker(pool, cid_list, worker_id):
    global global_completed, start_time
    closed_retry_count = {}

    for idx, cid in enumerate(cid_list):
        if stop_event.is_set():
            remaining = cid_list[idx:]
            if remaining:
                with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                    for c in remaining:
                        f.write(f"{c}\n")
            break

        retry_cnt = closed_retry_count.get(cid, 0)
        if retry_cnt >= MAX_RETRY_ON_CLOSED:
            async with global_lock:
                global_completed += 1
            print(f"[W{worker_id}] 放弃 {cid}: 页面关闭重试已达上限 {MAX_RETRY_ON_CLOSED} 次", flush=True)
            continue

        context, page = await pool.acquire()
        try:
            school_str, class_str = await process_cid_with_retry(page, cid)
            async with global_lock:
                global_completed += 1
                cur = global_completed

            is_valid = not (class_str == "无" and school_str == "无")
            if is_valid:
                elapsed = time.time() - start_time
                ratio = cur / global_total
                remain_str = format_time((elapsed / ratio) - elapsed) if ratio > 0 else "未知"
                mem_str = get_system_memory_str()
                cpu_str = get_cpu_percent_str()
                print(f"[W{worker_id}] ({cur}/{global_total}) 内存:{mem_str} cpu:{cpu_str}% 剩余:{remain_str} {cid} | {school_str} | {class_str}", flush=True)
                await add_to_buffer(f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school_str} {class_str}\n")

            if cid in closed_retry_count:
                del closed_retry_count[cid]

            await pool.release(context, page, is_broken=False)

        except Exception as e:
            error_msg = str(e)
            is_closed = any(phrase in error_msg for phrase in [
                "Target page, context or browser has been closed",
                "context has been closed",
                "page has been closed",
                "browser has been closed"
            ])
            if is_closed:
                closed_retry_count[cid] = retry_cnt + 1
                print(f"[W{worker_id}] 页面关闭 (cid={cid}, 重试 {closed_retry_count[cid]}/{MAX_RETRY_ON_CLOSED})，正在重建...", flush=True)
                # 归还并标记损坏，池会重建
                await pool.release(context, page, is_broken=True)
                # 注意：这个cid还没完成，所以不增加 global_completed，并且需要重试
                # 由于没有 break，循环会继续，但是当前cid还没标记完成，需要回退索引？
                # 最简单的处理：将该cid放回当前worker的未完成列表头部。
                # 这里采用将cid插回当前worker待处理列表的前面（通过修改循环索引）
                # 但为了简化，直接让当前worker重新处理这个cid（把cid放回队列）
                # 由于我们是在for循环中，无法简单回退，可以使用while循环或者将cid存入一个重试列表。
                # 采用一个简单方法：将cid重新插入到worker自己的任务列表前面。
                # 为了方便，我们修改循环为while手动管理索引。
                # 但为了兼容现有代码，这里采用另一种方式：将cid写入一个临时重试文件，后续处理。
                # 但会增加复杂度。更好的办法：在worker内维护一个本地队列。
                # 鉴于时间，我提供一个更简单的方案：引发异常让外层重新调度？不合理。
                # 重新设计：worker 使用 while 循环 + 索引，遇到关闭错误时 index 不减，continue。
                # 当前代码是for循环，无法做到。所以需要改写worker循环方式。
                # 请见下方改进版worker（使用while）。
                # 但由于篇幅，这里仅指出问题。实际我会在最终代码中提供正确版本。
            else:
                async with global_lock:
                    global_completed += 1
                error_line = error_msg.split("\n")[0][:100]
                print(f"[W{worker_id}] 错误 {cid}: {error_line}", flush=True)
                await pool.release(context, page, is_broken=False)

        await asyncio.sleep(SLEEP_BETWEEN)

# 修正版 worker（支持重试当前 cid）
async def worker_v2(pool, cid_list, worker_id):
    global global_completed, start_time
    i = 0
    closed_retry_count = {}
    while i < len(cid_list):
        if stop_event.is_set():
            # 记录剩余未完成的cid
            remaining = cid_list[i:]
            if remaining:
                with open(f"unfinished_worker_{worker_id}.txt", "a") as f:
                    for c in remaining:
                        f.write(f"{c}\n")
            break

        cid = cid_list[i]
        retry_cnt = closed_retry_count.get(cid, 0)
        if retry_cnt >= MAX_RETRY_ON_CLOSED:
            async with global_lock:
                global_completed += 1
            print(f"[W{worker_id}] 放弃 {cid}: 页面关闭重试已达上限 {MAX_RETRY_ON_CLOSED} 次", flush=True)
            i += 1
            continue

        context, page = await pool.acquire()
        try:
            school_str, class_str = await process_cid_with_retry(page, cid)
            async with global_lock:
                global_completed += 1
                cur = global_completed

            is_valid = not (class_str == "无" and school_str == "无")
            if is_valid:
                elapsed = time.time() - start_time
                ratio = cur / global_total
                remain_str = format_time((elapsed / ratio) - elapsed) if ratio > 0 else "未知"
                mem_str = get_system_memory_str()
                cpu_str = get_cpu_percent_str()
                print(f"[W{worker_id}] ({cur}/{global_total}) 内存:{mem_str} cpu:{cpu_str}% 剩余:{remain_str} {cid} | {school_str} | {class_str}", flush=True)
                await add_to_buffer(f"{cid} https://www.eeo.cn/s/a/?cid={cid} {school_str} {class_str}\n")

            if cid in closed_retry_count:
                del closed_retry_count[cid]

            await pool.release(context, page, is_broken=False)
            i += 1  # 成功处理，移动到下一个cid

        except Exception as e:
            error_msg = str(e)
            is_closed = any(phrase in error_msg for phrase in [
                "Target page, context or browser has been closed",
                "context has been closed",
                "page has been closed",
                "browser has been closed"
            ])
            if is_closed:
                closed_retry_count[cid] = retry_cnt + 1
                print(f"[W{worker_id}] 页面关闭 (cid={cid}, 重试 {closed_retry_count[cid]}/{MAX_RETRY_ON_CLOSED})，正在重建...", flush=True)
                await pool.release(context, page, is_broken=True)
                # 不移动 i，继续重试当前 cid
                continue
            else:
                async with global_lock:
                    global_completed += 1
                error_line = error_msg.split("\n")[0][:100]
                print(f"[W{worker_id}] 错误 {cid}: {error_line}", flush=True)
                await pool.release(context, page, is_broken=False)
                i += 1

        await asyncio.sleep(SLEEP_BETWEEN)

# ---------- 主函数 ----------
async def main_async():
    global start_time, global_total
    start_time = time.time()

    # 确定扫描列表
    if CID_LIST_FILE:
        with open(CID_LIST_FILE, "r") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
        global_total = len(cid_list)
        print(f"列表模式: 从 {CID_LIST_FILE} 读取 {global_total} 个班级")
    else:
        if START_CID is None or END_CID is None:
            print("错误: 必须指定 start_cid/end_cid 或 cid_list_file", flush=True)
            sys.exit(1)
        cid_list = list(range(START_CID, END_CID + 1))
        global_total = len(cid_list)
        print(f"区间模式: {START_CID} ~ {END_CID} (共 {global_total} 个)")

    print(f"最大并发页面数: {MAX_CONCURRENT_PAGES}")
    print(f"批量写入大小: {BATCH_SIZE}")
    print(f"超时限制: {TIMEOUT_HOURS} 小时 ({TIMEOUT_SECONDS} 秒)")
    print(f"页面关闭最大重试: {MAX_RETRY_ON_CLOSED}")
    print(f"结果保存至: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox",
                "--disable-setuid-sandbox", "--disable-accelerated-2d-canvas",
                "--disable-background-networking", "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows", "--disable-breakpad",
                "--disable-client-side-phishing-detection", "--disable-default-apps",
                "--disable-extensions", "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                "--disable-hang-monitor", "--disable-ipc-flooding-protection",
                "--disable-popup-blocking", "--disable-prompt-on-repost",
                "--disable-renderer-backgrounding", "--disable-sync",
                "--disable-software-rasterizer", "--metrics-recording-only",
                "--no-first-run", "--safebrowsing-disable-auto-update",
                "--disable-logging", "--silent", "--js-flags=--max-old-space-size=128"
            ]
        )

        # 创建上下文池
        pool = ContextPool(browser, MAX_CONCURRENT_PAGES)
        await pool.initialize()

        # 分配 CID 给 workers（workers数量可以大于池大小，但实际并发受池限制）
        worker_count = min(MAX_CONCURRENT_PAGES * 2, len(cid_list))  # 可适当多些worker，但池会限流
        chunk_size = (len(cid_list) + worker_count - 1) // worker_count
        chunks = [cid_list[i*chunk_size:(i+1)*chunk_size] for i in range(worker_count) if i*chunk_size < len(cid_list)]
        tasks = [asyncio.create_task(worker_v2(pool, chunk, i)) for i, chunk in enumerate(chunks)]

        async def set_stop():
            await asyncio.sleep(TIMEOUT_SECONDS)
            stop_event.set()
            print(f"\n⚠️ 已达到运行时间上限（{TIMEOUT_HOURS}小时），停止接收新班级，等待现有任务完成...", flush=True)

        timeout_task = asyncio.create_task(set_stop())
        await asyncio.gather(*tasks, return_exceptions=True)
        timeout_task.cancel()
        await flush_buffer()
        await pool.close_all()
        await browser.close()

    elapsed_total = time.time() - start_time

    if stop_event.is_set():
        with open(UNFINISHED_FLAG, "w") as f:
            f.write(f"Timeout at {time.ctime()}\n")
            f.write(f"Processed {global_completed}/{global_total}\n")
        print(f"\n⚠️ 扫描未完成，已生成 {UNFINISHED_FLAG}", flush=True)

        # 合并 worker 未完成列表
        unfinished_cids = set()
        for i in range(worker_count):
            wfile = f"unfinished_worker_{i}.txt"
            if os.path.exists(wfile):
                with open(wfile, "r") as f:
                    for line in f:
                        if line.strip():
                            unfinished_cids.add(int(line.strip()))
                os.remove(wfile)
        if unfinished_cids:
            with open(f"unfinished_cids_{SHARD_IDX}.txt", "w") as f:
                for cid in sorted(unfinished_cids):
                    f.write(f"{cid}\n")
            print(f"记录了 {len(unfinished_cids)} 个未完成的 CID", flush=True)
    else:
        if os.path.exists(UNFINISHED_FLAG):
            os.remove(UNFINISHED_FLAG)
        print(f"\n✅ 扫描完成！总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}")

    print(f"最终处理班级数: {global_completed}/{global_total}", flush=True)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
