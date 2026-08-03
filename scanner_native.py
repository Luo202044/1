#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import random
import asyncio
import traceback
import psutil
from playwright.async_api import async_playwright

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

# ========== 硬件适配参数 ==========
# 🔥 硬核限制：4 核机器最优总并发为 36～40，超出会 CPU 抖动
MAX_CONCURRENT = min(config.get("max_concurrent_pages", 36), 40)

# 超时时间（单位：秒），可从配置读取，默认 3 小时
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

# 🚨 修复: 补回了遗漏的 USER_AGENTS 列表
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ========== 使用 Obscura 时的 CDP 连接地址 ==========
OBSCURA_CDP_URL = "http://localhost:9222"

# ========== 微任务防抖：针对完整 Vue.js 渲染设计的秒退算法 ==========
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

        // 🔥 超时从 3200ms 提升到 8000ms，防止 CPU 拥堵时过早丢弃
        let timeoutId = setTimeout(() => {
            observer.disconnect();
            resolve({status: "timeout"});
        }, 8000); 
    });
}"""

# ========== 异步工作器（单进程高并发） ==========
async def async_worker(cid_chunk, concurrency, deadline, shared_counter):
    """
    单进程异步工作器，连接到 Obscura CDP 服务。
    """
    in_flight_cids = set()
    results = []
    local_queue = asyncio.Queue()
    
    for cid in cid_chunk:
        local_queue.put_nowait(cid)
        
    async def fetcher(coro_id, context, browser):
        page = None
        
        async def init_page():
            nonlocal page
            if page:
                try: await page.close()
                except: pass
            page = await context.new_page()
            # 可选：设置 User-Agent（可通过 context 统一设置，也可在页面设置）
        
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
                # 超时时间延长至 18 秒，防止 CPU 抖动下过早超时
                pw_timeout = min(18000, (deadline - time.time()) * 1000)
                await page.goto(f"https://www.eeo.cn/s/a/?cid={cid}", timeout=pw_timeout, wait_until="commit")
                
                data = await page.evaluate(js_extract_promise)
                
                # 尝试提前停止加载（非必需，若 Obscura 不支持则忽略）
                try:
                    await page.evaluate("window.stop()")
                except:
                    pass

                status = data.get('status', 'timeout')
                
                if status == 'waf':
                    if random.random() < 0.1: 
                        print(f"⚠️ [风控侦测] C{coro_id:02d} 遭遇拦截，正在休眠避让...", flush=True)
                    raise Exception("WAF_BLOCKED")
                    
                elif status == 'success':
                    class_name = data.get('class_name', '无')
                    school = data.get('school', '无')
                    teacher = data.get('teacher', '无')
                    invalid_marks = {"无", "-", "--", "---", "—", "_", ""}
                    if not (class_name in invalid_marks and school in invalid_marks):
                        line = f"{cid}\thttps://www.eeo.cn/s/a/?cid={cid}\t{school}\t{teacher}\t{class_name}\n"
                        results.append(line)
                        print(f"✅ [满血解析] C{coro_id:02d} | {cid} | 🏫 {school} | 🧑‍🏫 {teacher} | 🎓 {class_name}", flush=True)

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

            # 每 100 个请求刷新一次页面（防止内存泄漏）
            if lifecycle >= 100:
                await init_page()
                lifecycle = 0

            # 批量写入结果
            if len(results) >= 100:
                with open(f"data/proc_temp.txt", "a", encoding="utf-8") as f:
                    f.writelines(results)
                results.clear()

        if page:
            try: await page.close()
            except: pass

    # 连接到 Obscura CDP
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(OBSCURA_CDP_URL)
        except Exception as e:
            print(f"❌ 无法连接到 Obscura CDP 服务 ({OBSCURA_CDP_URL}): {e}", flush=True)
            print("请确保 Obscura 服务已启动并监听 9222 端口", flush=True)
            return

        # 获取已有上下文，或创建新上下文
        if browser.contexts:
            context = browser.contexts[0]
        else:
            context = await browser.new_context()

        # 统一设置 User-Agent
        await context.set_extra_http_headers({
            "User-Agent": random.choice(USER_AGENTS)
        })

        tasks = [asyncio.create_task(fetcher(i, context, browser)) for i in range(concurrency)]
        
        # 等待队列清空或超时
        wait_task = asyncio.create_task(local_queue.join())
        try:
            timeout_wait = deadline - time.time()
            if timeout_wait > 0:
                await asyncio.wait_for(wait_task, timeout=timeout_wait)
        except asyncio.TimeoutError:
            print("⏱️ 总超时到达，停止接收新任务", flush=True)

        # 取消所有 fetcher 任务
        for t in tasks:
            t.cancel()
        
        # 关闭连接（不影响 Obscura 服务本身）
        await context.close()
        await browser.close()

    # 写入剩余结果
    if results:
        with open(f"data/proc_temp.txt", "a", encoding="utf-8") as f:
            f.writelines(results)

    # 收集未完成的 CID（留在队列中的）
    while not local_queue.empty():
        in_flight_cids.add(local_queue.get_nowait())
        
    if in_flight_cids:
        with open(f"unfinished_proc.txt", "w", encoding="utf-8") as f:
            for cid in in_flight_cids:
                f.write(f"{cid}\n")

# ========== 主函数 ==========
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
    # 🔥 单进程异步，总并发 = MAX_CONCURRENT
    concurrency = min(MAX_CONCURRENT, total_tasks)
    
    print(f"🚀 [Obscura 轻量引擎] 启动！全面支持 Vue.js 渲染", flush=True)
    print(f"⚙️  配置: 总并发 {concurrency} | 超时 {TIMEOUT_SECONDS/3600:.1f}h", flush=True)

    shared_counter = mp.Value('i', 0)
    deadline = time.time() + TIMEOUT_SECONDS - 60  # 预留 60 秒清理时间
    start_time = time.time()
    
    # 运行异步工作器（直接运行，无多进程）
    asyncio.run(async_worker(cid_list, concurrency, deadline, shared_counter))

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

    # 处理未完成的 CID
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
    else:
        print("✅ 所有 CID 已完美处理完毕！")

    os._exit(0)

if __name__ == "__main__":
    # 由于不再使用多进程，不需要 set_start_method，但为了兼容保留
    import multiprocessing as mp
    try:
        mp.set_start_method('spawn')
    except:
        pass
    main()