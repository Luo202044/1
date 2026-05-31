#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import re
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ========== 内存监控相关 ==========
try:
    import psutil
    PSUTIL_AVAILABLE = True
    _process = psutil.Process(os.getpid())
    TOTAL_MEM_GB = psutil.virtual_memory().total / (1024 ** 3)
except ImportError:
    PSUTIL_AVAILABLE = False
    _process = None
    TOTAL_MEM_GB = None

def get_memory_str():
    """返回当前进程内存/总内存 字符串，例如 '0.52GB/7.0GB'；若不可用返回 'N/A'"""
    if not PSUTIL_AVAILABLE:
        return "N/A"
    try:
        mem_bytes = _process.memory_info().rss
        mem_gb = mem_bytes / (1024 ** 3)
        return f"{mem_gb:.2f}GB/{TOTAL_MEM_GB:.1f}GB"
    except Exception:
        return "?GB/?GB"

# ========== 加载配置 ==========
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

START_CID = config["start_cid"]
END_CID = config["end_cid"]
REQUESTED_THREADS = config.get("threads", 3)
MAX_TASKS_PER_DRIVER = config.get("max_tasks_per_driver", 20)   # 建议20
WAIT_TIMEOUT = config.get("wait_timeout", 5)                    # 降至5秒
RENDER_WAIT = config.get("render_wait", 0.2)                    # 0.2秒显式等待
SLEEP_BETWEEN = config.get("sleep_between", 0.02)               # 线程间休眠

# 内存自适应（若 psutil 可用则根据可用内存调整线程数）
if PSUTIL_AVAILABLE:
    mem = psutil.virtual_memory()
    available_gb = mem.available / (1024**3)
    if available_gb < 2.0:
        RECOMMENDED = 2
        print(f"检测到可用内存仅 {available_gb:.1f}GB，自动将线程数从 {REQUESTED_THREADS} 降至 {RECOMMENDED}", flush=True)
        THREADS = RECOMMENDED
    else:
        THREADS = REQUESTED_THREADS
else:
    THREADS = REQUESTED_THREADS
    print("未安装psutil，无法自动检测内存，请手动确保线程数不超过20", flush=True)

os.makedirs("data", exist_ok=True)
OUTPUT_FILE = os.path.join("data", f"{START_CID}-{END_CID}.txt")

# 全局进度计数器
global_total = END_CID - START_CID + 1
global_completed = 0
global_lock = threading.Lock()
start_time = None

thread_local = threading.local()
drivers_lock = threading.Lock()
all_drivers = []

def format_time(seconds):
    if seconds < 0:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def create_driver(retries=2):
    opts = Options()
    # 基础参数
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    
    # 内存与性能优化
    opts.add_argument("--js-flags=--max-old-space-size=128")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-breakpad")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-features=TranslateUI,BlinkGenPropertyTrees")
    opts.add_argument("--disable-hang-monitor")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-prompt-on-repost")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-sync")
    opts.add_argument("--metrics-recording-only")
    opts.add_argument("--no-first-run")
    opts.add_argument("--safebrowsing-disable-auto-update")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-logging")
    opts.add_argument("--silent")
    
    # 禁用图片、CSS、字体等
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.media_stream": 2,
        "profile.default_content_settings.popups": 2,
        "profile.managed_default_content_settings.plugins": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    
    for attempt in range(retries):
        try:
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(WAIT_TIMEOUT)
            return driver
        except Exception as e:
            print(f"创建Chrome驱动失败 (尝试 {attempt+1}/{retries}): {e}", flush=True)
            if attempt == retries-1:
                raise
            time.sleep(2)
    return None

def restart_driver(thread_name):
    if hasattr(thread_local, "driver"):
        try:
            thread_local.driver.quit()
            with drivers_lock:
                if thread_local.driver in all_drivers:
                    all_drivers.remove(thread_local.driver)
        except:
            pass
        delattr(thread_local, "driver")
    thread_local.driver = create_driver()
    thread_local.task_count = 0
    with drivers_lock:
        all_drivers.append(thread_local.driver)
    print(f"[{thread_name}] 浏览器已启动", flush=True)

def get_driver(thread_name):
    if not hasattr(thread_local, "driver"):
        restart_driver(thread_name)
    elif hasattr(thread_local, "task_count") and thread_local.task_count >= MAX_TASKS_PER_DRIVER:
        print(f"[{thread_name}] 已处理 {thread_local.task_count} 个班级，达到阈值，重启浏览器", flush=True)
        restart_driver(thread_name)
    return thread_local.driver

def extract_class_name(driver):
    try:
        elem = driver.find_element(By.CSS_SELECTOR, "p.courseName")
        name = elem.text.strip()
        if name and len(name) >= 2:
            return name
    except:
        pass
    title = driver.title
    if "|" in title and "Join the class" not in title:
        parts = title.split("|")
        if len(parts) > 1:
            return parts[-1].strip()
    return None

def extract_school_name(driver):
    try:
        elem = driver.find_element(By.CSS_SELECTOR, "p.schoolName")
        name = elem.text.strip()
        if name and len(name) >= 2:
            return name
    except:
        pass
    return None

def process_cid(cid, thread_name, total_tasks):
    global global_completed, global_total, start_time
    if not hasattr(thread_local, "completed"):
        thread_local.completed = 0
        thread_local.total = total_tasks
    driver = get_driver(thread_name)
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    try:
        # 清除缓存避免内存累积
        try:
            driver.delete_all_cookies()
            driver.execute_script("window.localStorage.clear();")
            driver.execute_script("window.sessionStorage.clear();")
        except:
            pass
        
        driver.get(url)
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # 动态等待关键元素
        try:
            WebDriverWait(driver, RENDER_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "p.courseName, p.schoolName"))
            )
        except:
            pass
        
        class_name = extract_class_name(driver)
        school_name = extract_school_name(driver)

        class_str = class_name if class_name else "无"
        school_str = school_name if school_name else "无"

        thread_local.completed += 1
        with global_lock:
            global_completed += 1
            cur_global = global_completed

        # 估算剩余时间
        elapsed = time.time() - start_time
        ratio = cur_global / global_total
        if ratio > 0:
            remaining = (elapsed / ratio) - elapsed
            remain_str = format_time(remaining)
        else:
            remain_str = "未知"

        mem_str = get_memory_str()
        print(f"[{thread_name}] (工作进度：{thread_local.completed}/{thread_local.total}) (内存: {mem_str}) (总进度：{cur_global}/{global_total} 剩余时间：{remain_str}) {cid} | 机构: {school_str} | 班级: {class_str}", flush=True)

        if not (class_str == "无" and school_str == "无"):
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(f"{cid} {url} {school_str} {class_str}\n")
            return True
        return False
    except Exception as e:
        thread_local.completed += 1
        with global_lock:
            global_completed += 1
            cur_global = global_completed
        elapsed = time.time() - start_time
        ratio = cur_global / global_total if global_total > 0 else 0
        if ratio > 0:
            remaining = (elapsed / ratio) - elapsed
            remain_str = format_time(remaining)
        else:
            remain_str = "未知"
        mem_str = get_memory_str()
        if "timeout" in str(e).lower():
            print(f"[{thread_name}] (工作进度：{thread_local.completed}/{thread_local.total}) (内存: {mem_str}) (总进度：{cur_global}/{global_total} 剩余时间：{remain_str}) {cid} 超时", flush=True)
        else:
            print(f"[{thread_name}] (工作进度：{thread_local.completed}/{thread_local.total}) (内存: {mem_str}) (总进度：{cur_global}/{global_total} 剩余时间：{remain_str}) {cid} 错误: {e}", flush=True)
        return False
    finally:
        if hasattr(thread_local, "task_count"):
            thread_local.task_count += 1
        else:
            thread_local.task_count = 1
        time.sleep(SLEEP_BETWEEN)

def close_all_drivers():
    with drivers_lock:
        for driver in all_drivers:
            try:
                driver.quit()
            except:
                pass
        all_drivers.clear()

def main():
    global global_completed, global_total, start_time
    start_time = time.time()
    print(f"班级范围: {START_CID} - {END_CID}", flush=True)
    print(f"请求线程数: {REQUESTED_THREADS}, 实际使用: {THREADS}", flush=True)
    print(f"每浏览器最大任务: {MAX_TASKS_PER_DRIVER}", flush=True)
    total = END_CID - START_CID + 1
    print(f"总班级数: {total}", flush=True)
    print(f"结果将保存到: {OUTPUT_FILE}", flush=True)
    print("内存格式: 当前进程内存/机器总内存\n", flush=True)

    # 连续区间分配
    base_size = total // THREADS
    remainder = total % THREADS
    thread_ranges = []
    current = START_CID
    for i in range(THREADS):
        size = base_size + (1 if i < remainder else 0)
        if size > 0:
            end = current + size - 1
            thread_ranges.append((current, end))
            current = end + 1
        else:
            thread_ranges.append((0, -1))

    for i, (start, end) in enumerate(thread_ranges):
        if start <= end:
            print(f"T{i+1} 负责 {end-start+1} 个班级 (范围 {start} ~ {end})", flush=True)
        else:
            print(f"T{i+1} 负责 0 个班级", flush=True)
    print(flush=True)

    # 清空输出文件
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")

    valid = 0
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for i, (start, end) in enumerate(thread_ranges):
            if start > end:
                continue
            thread_name = f"T{i+1}"
            total_tasks = end - start + 1
            for cid in range(start, end+1):
                futures.append(executor.submit(process_cid, cid, thread_name, total_tasks))
        for future in as_completed(futures):
            if future.result():
                valid += 1

    close_all_drivers()
    elapsed_total = time.time() - start_time
    print(f"\n探测完成！有效班级数: {valid}，总耗时: {format_time(elapsed_total)}，结果保存至 {OUTPUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
