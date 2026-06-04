#!/usr/bin/env python3
import os
import json
import glob

def main():
    retry_indices = []
    flags_dir = "flags"
    if os.path.isdir(flags_dir):
        for dirpath in glob.glob(f"{flags_dir}/flag-*/"):
            # 检查是否存在以 unfinished_ 开头的文件
            if glob.glob(os.path.join(dirpath, "unfinished_*")):
                idx = os.path.basename(dirpath.rstrip('/')).replace('flag-', '')
                retry_indices.append(idx)
    output = json.dumps(retry_indices)
    # 写入 GitHub Output
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f"retry_list={output}\n")

if __name__ == "__main__":
    main()
