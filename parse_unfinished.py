#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
解析未完成分片标志，生成 retry_list JSON 数组。
该脚本由 GitHub Actions 的 collect_unfinished job 调用。
"""

import os
import json
import glob
import sys

def main():
    retry_indices = []
    flags_dir = "flags"

    # 1. 检查 flags 目录是否存在
    if not os.path.isdir(flags_dir):
        print(f"ERROR: Directory '{flags_dir}' does not exist.", file=sys.stderr)
        output = json.dumps([])
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"retry_list={output}\n")
        return

    # 2. 遍历所有 flag-* 子目录
    flag_subdirs = glob.glob(os.path.join(flags_dir, "flag-*/"))
    print(f"Found flag subdirs: {flag_subdirs}", file=sys.stderr)

    for dirpath in flag_subdirs:
        # 查找该目录下所有以 unfinished_ 开头的文件（不限后缀）
        unfinished_files = glob.glob(os.path.join(dirpath, "unfinished_*"))
        if unfinished_files:
            # 提取分片索引：目录名 "flag-0" -> "0"
            idx = os.path.basename(dirpath.rstrip('/')).replace('flag-', '')
            retry_indices.append(idx)
            print(f"Unfinished shard detected: {idx} (files: {unfinished_files})", file=sys.stderr)
        else:
            print(f"No unfinished file in {dirpath}", file=sys.stderr)

    # 3. 生成 JSON 数组字符串
    output = json.dumps(retry_indices)
    print(f"retry_list = {output}", file=sys.stderr)

    # 4. 写入 GITHUB_OUTPUT
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f"retry_list={output}\n")

if __name__ == "__main__":
    main()
