#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ========== 加载配置 ==========
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

START_CID = config["start_cid"]
END_CID = config["end_cid"]
THREADS = config.get("threads", 3)
MAX_TASKS_PER_DRIVER = config.get("max_tasks_per_driver", 30)
WAIT_TIMEOUT = config.get("wait_timeout", 10)
RENDER_WAIT = config.get("render_wait", 0.8)
SLEEP_BETWEEN = config.get("sleep_between", 0.3)
# =============================

# 输出文件名
OUTPUT_FILE = f"{START_CID}-{END_CID}.txt"

# 线程局部存储
thread_local = threading.local()
drivers_lock = threading.Lock()
all_drivers = []

def create_driver():
    """创建无头 Chrome 驱动（禁用图片，最小内存）"""
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
    })
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(WAIT_TIMEOUT)
    return driver

def restart_driver(thread_name):
    """关闭当前线程的 driver 并重新创建（内存回收）"""
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
    print(f"[{thread_name}] 浏览器实例已重启（内存回收）")

def get_driver(thread_name):
    if not hasattr(thread_local, "driver"):
        restart_driver(thread_name)
    elif hasattr(thread_local, "task_count") and thread_local.task_count >= MAX_TASKS_PER_DRIVER:
        print(f"[{thread_name}] 已处理 {thread_local.task_count} 个班级，达到阈值 {MAX_TASKS_PER_DRIVER}，重启浏览器...")
        restart_driver(thread_name)
    return thread_local.driver

def extract_teacher(driver):
    teacher = None
    try:
        elem = driver.find_element(By.CSS_SELECTOR, "div.courseTeacher span")
        text = elem.text.strip()
        if re.match(r'1[3-9]\d{9}$', text) or re.match(r'1\d{2}\*{4}\d{4}$', text):
            teacher = text
    except:
        pass
    if not teacher:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, "div.courseTeacher")
            full_text = elem.text
            if "教师：" in full_text:
                candidate = full_text.split("教师：")[-1].strip()
                if re.match(r'1[3-9]\d{9}$', candidate) or re.match(r'1\d{2}\*{4}\d{4}$', candidate):
                    teacher = candidate
        except:
            pass
    return teacher

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

def process_cid(cid, thread_name):
    driver = get_driver(thread_name)
    url = f"https://www.eeo.cn/s/a/?cid={cid}"
    try:
        driver.get(url)
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(RENDER_WAIT)
        teacher = extract_teacher(driver)
        class_name = extract_class_name(driver)
        school_name = extract_school_name(driver)
        teacher_str = teacher if teacher else "无"
        class_str = class_name if class_name else "无"
        school_str = school_name if school_name else "无"
        print(f"[{thread_name}] {cid} | 教师: {teacher_str} | 班级: {class_str} | 机构: {school_str}")
        if teacher:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(f"{cid} {url} {teacher} {school_str} {class_str}\n")
            return True
        return False
    except Exception as e:
        print(f"[{thread_name}] {cid} 错误: {e}")
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
    print(f"班级范围: {START_CID} - {END_CID}")
    print(f"线程数: {THREADS}, 每浏览器最大任务数: {MAX_TASKS_PER_DRIVER}")
    total = END_CID - START_CID + 1
    cid_list = list(range(START_CID, END_CID+1))
    print(f"总班级数: {total}\n")
    # 清空输出文件
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("")
    valid = 0
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(process_cid, cid, f"T{i%THREADS+1}"): cid for i, cid in enumerate(cid_list)}
        for future in as_completed(futures):
            if future.result():
                valid += 1
    close_all_drivers()
    print(f"\n探测完成！有效班级数: {valid}，结果保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
