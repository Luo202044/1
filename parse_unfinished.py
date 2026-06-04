#!/usr/bin/env python3
import os
import json
import glob
import sys

def main():
    retry_indices = []
    flags_dir = "flags"
    if not os.path.isdir(flags_dir):
        print(f"ERROR: Directory '{flags_dir}' does not exist.", file=sys.stderr)
        output = json.dumps([])
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"retry_list={output}\n")
        return

    flag_subdirs = glob.glob(os.path.join(flags_dir, "flag-*/"))
    print(f"Found flag subdirs: {flag_subdirs}", file=sys.stderr)

    for dirpath in flag_subdirs:
        unfinished_files = glob.glob(os.path.join(dirpath, "unfinished_*"))
        if unfinished_files:
            idx = os.path.basename(dirpath.rstrip('/')).replace('flag-', '')
            retry_indices.append(idx)
            print(f"Unfinished shard detected: {idx} (files: {unfinished_files})", file=sys.stderr)
        else:
            print(f"No unfinished file in {dirpath}", file=sys.stderr)

    output = json.dumps(retry_indices)
    print(f"retry_list = {output}", file=sys.stderr)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f"retry_list={output}\n")

if __name__ == "__main__":
    main()
