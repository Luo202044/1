#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主控脚本，支持区间模式（range）和文件模式（file）。
使用 subprocess 启动多个 worker_sync.py 进程，并为每个进程设置超时。
"""

import os
import sys
import json
import argparse
import subprocess
import math
import time
import glob
from multiprocessing import cpu_count

# ---------- 默认配置 ----------
DEFAULT_CONFIG = {
    "max_concurrent": 20,
    "wait_timeout": 25,
    "render_wait": 2.0,
    "sleep_between": 0.3,
    "retry_times": 2,
    "retry_delay": 1.0,
    "batch_size": 200,
    "max_retry_on_closed": 3,
    "worker_process_timeout": 600   # 每个worker进程超时（秒）
}

def load_config():
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            user_config = json.load(f)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update(user_config)
        return cfg
    else:
        print("警告: config.json 不存在，使用默认配置", flush=True)
        return DEFAULT_CONFIG.copy()

def split_list(lst, n):
    """将列表分成n份"""
    k, m = divmod(len(lst), n)
    return [lst[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

def run_worker_process(worker_id, cid_chunk, config, proxy_list, user_agents, output_dir, timeout_sec):
    """启动单个Worker子进程，超时则终止"""
    cmd = [
        sys.executable, "worker_sync.py",
        str(worker_id),
        json.dumps(cid_chunk),
        json.dumps(config),
        json.dumps(proxy_list),
        json.dumps(user_agents),
        output_dir
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = proc.communicate(timeout=timeout_sec)
        if proc.returncode != 0:
            print(f"Worker {worker_id} 异常退出，返回码 {proc.returncode}", flush=True)
            if stderr:
                print(stderr.decode(), flush=True)
            return False
        return True
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        print(f"Worker {worker_id} 超时({timeout_sec}秒)，已强制终止", flush=True)
        # 注意：超时的worker可能已经部分写入unfinished文件，剩余未处理的CID会在最终合并时被记录
        return False

def scan_cid_list(cid_list, config, output_dir, worker_timeout, max_workers=None):
    """扫描CID列表，返回成功处理的CID集合和未完成CID集合"""
    if max_workers is None:
        max_workers = config.get("max_concurrent", 20)
    worker_count = min(max_workers, len(cid_list))
    if worker_count == 0:
        return set(), set()

    # 分片
    chunks = split_list(cid_list, worker_count)
    # 准备代理和UA（从config或环境变量读取，这里使用空列表）
    proxy_list = []
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # 启动所有Worker进程
    results = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        print(f"启动 Worker {i}，处理 {len(chunk)} 个CID", flush=True)
        success = run_worker_process(i, chunk, config, proxy_list, user_agents, output_dir, worker_timeout)
        results.append(success)

    # 收集所有临时输出文件
    all_valid_lines = []
    for i in range(len(chunks)):
        tmp_file = os.path.join(output_dir, f"output_{i}.txt")
        if os.path.exists(tmp_file):
            with open(tmp_file, "r") as f:
                all_valid_lines.extend(f.readlines())
            os.remove(tmp_file)

    # 合并去重
    seen = set()
    valid_cids = set()
    merged_lines = []
    for line in all_valid_lines:
        parts = line.split()
        if parts:
            cid = parts[0]
            if cid not in seen:
                seen.add(cid)
                valid_cids.add(cid)
                merged_lines.append(line)

    # 写最终输出
    final_output = os.path.join(output_dir, "merged_output.txt")
    with open(final_output, "w", encoding="utf-8") as f:
        f.write("".join(merged_lines))
    print(f"合并有效班级 {len(valid_cids)} 条，保存至 {final_output}", flush=True)

    # 收集所有未完成CID
    unfinished_cids = set()
    for i in range(len(chunks)):
        unfin_file = f"unfinished_worker_{i}.txt"
        if os.path.exists(unfin_file):
            with open(unfin_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and line.isdigit():
                        unfinished_cids.add(int(line))
            os.remove(unfin_file)

    return valid_cids, unfinished_cids

def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--mode", choices=["range", "file"], help="range: 区间扫描; file: 文件列表")
    parser.add_argument("--start", type=int, help="起始CID (range模式)")
    parser.add_argument("--end", type=int, help="结束CID (range模式)")
    parser.add_argument("--file", type=str, help="CID列表文件 (file模式)")
    parser.add_argument("--output", default="data", help="输出目录，默认data")
    args = parser.parse_args()

    config = load_config()
    worker_timeout = config.get("worker_process_timeout", 600)

    # 确定CID列表
    if args.mode == "range":
        if args.start is None or args.end is None:
            print("错误: range模式需要 --start 和 --end", flush=True)
            sys.exit(1)
        cid_list = list(range(args.start, args.end + 1))
        print(f"区间模式: {args.start} ~ {args.end} (共 {len(cid_list)} 个)", flush=True)
        shard_idx = os.environ.get("SHARD_IDX", "unknown")
        output_dir = os.path.join(args.output, f"range_{args.start}_{args.end}")
    else:  # file模式
        if not args.file or not os.path.exists(args.file):
            print(f"错误: 文件 {args.file} 不存在", flush=True)
            sys.exit(1)
        with open(args.file, "r") as f:
            cid_list = [int(line.strip()) for line in f if line.strip()]
        print(f"文件模式: 从 {args.file} 读取 {len(cid_list)} 个班级", flush=True)
        shard_idx = os.environ.get("SHARD_IDX", os.path.basename(args.file).replace(".txt", ""))
        output_dir = os.path.join(args.output, f"file_{shard_idx}")

    os.makedirs(output_dir, exist_ok=True)

    # 执行扫描
    start_time = time.time()
    valid_cids, unfinished_cids = scan_cid_list(cid_list, config, output_dir, worker_timeout)
    elapsed = time.time() - start_time

    # 输出未完成CID列表（供后续重扫）
    if unfinished_cids:
        unfin_file = f"unfinished_cids_{shard_idx}.txt"
        with open(unfin_file, "w") as f:
            for cid in sorted(unfinished_cids):
                f.write(f"{cid}\n")
        print(f"未完成CID数: {len(unfinished_cids)}，已保存至 {unfin_file}", flush=True)
    else:
        print("所有CID均已成功处理", flush=True)

    print(f"扫描完成，耗时 {elapsed:.2f} 秒", flush=True)

if __name__ == "__main__":
    main()
