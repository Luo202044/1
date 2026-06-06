#!/usr/bin/env python3
import os
import json
import glob
import math

def main():
    cid_files = glob.glob("unfinished_cids_*.txt")
    all_cids = set()
    for f in cid_files:
        with open(f, 'r', encoding='utf-8') as fp:
            for line in fp:
                line = line.strip()
                if line:
                    all_cids.add(int(line))
    cid_list = sorted(all_cids)
    total = len(cid_list)
    print(f"Total unfinished CIDs: {total}")

    shards = int(os.environ.get('RETRY_SHARDS', '20'))
    if total == 0:
        shard_indices = []
    else:
        chunk_size = math.ceil(total / shards)
        os.makedirs("retry_shards", exist_ok=True)
        shard_indices = []
        for i in range(shards):
            start = i * chunk_size
            end = min((i+1)*chunk_size, total)
            if start >= total:
                break
            subset = cid_list[start:end]
            out_file = f"retry_shards/retry_cids_{i}.txt"
            with open(out_file, 'w', encoding='utf-8') as f:
                for cid in subset:
                    f.write(f"{cid}\n")
            shard_indices.append(str(i))
            print(f"Written {len(subset)} CIDs to {out_file}")

    if 'GITHUB_OUTPUT' in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as f:
            f.write(f"retry_shard_list={json.dumps(shard_indices)}\n")
    else:
        print(f"retry_shard_list={json.dumps(shard_indices)}")

if __name__ == "__main__":
    main()
