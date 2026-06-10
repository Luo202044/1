import os
import re
import sys
from datetime import datetime
from collections import defaultdict

# Configuration
blocked_orgs = ["中山搏浪教育", "云趣教育", "无忧伴学"]
blocked_match_mode = "exact"
dedup_keep_strategy = "first"
confirm_before_write = True

# Check for --no-confirm flag
if "--no-confirm" in sys.argv:
    confirm_before_write = False

# Data directory
data_dir = r"d:\99\5\1\data"

def parse_line(line):
    """Parse a line according to the specified format."""
    line = line.rstrip('\n\r')
    
    # Skip empty lines or whitespace-only lines
    if not line.strip():
        return None, None, None, None, "empty"
    
    # Skip comment lines
    if line.strip().startswith('#'):
        return None, None, None, None, "comment"
    
    # Parse the line
    parts = line.split(' ')
    if len(parts) < 3:
        return None, None, None, None, "unparseable"
    
    # First space before → ID
    first_space_idx = line.find(' ')
    if first_space_idx == -1:
        return None, None, None, None, "unparseable"
    
    id_str = line[:first_space_idx]
    
    # Second space before → URL
    second_space_idx = line.find(' ', first_space_idx + 1)
    if second_space_idx == -1:
        return None, None, None, None, "unparseable"
    
    url = line[first_space_idx + 1:second_space_idx]
    
    # Last space → split org name and class name
    last_space_idx = line.rfind(' ')
    if last_space_idx == -1 or last_space_idx <= second_space_idx:
        return None, None, None, None, "unparseable"
    
    # From second space to last space → Organization Name
    org_name = line[second_space_idx + 1:last_space_idx]
    
    # After last space → Class Name
    class_name = line[last_space_idx + 1:]
    
    # Try to parse ID as number
    try:
        id_num = int(id_str)
    except ValueError:
        id_num = None
    
    return id_str, url, org_name, class_name, "parsed"

def is_blocked_org(org_name):
    """Check if organization name is blocked."""
    if blocked_match_mode == "exact":
        return org_name in blocked_orgs
    elif blocked_match_mode == "contains":
        return any(blocked in org_name for blocked in blocked_orgs)
    elif blocked_match_mode == "regex":
        return any(re.search(blocked, org_name) for blocked in blocked_orgs)
    return False

def collect_files():
    """Collect all class_*.txt files sorted by filename."""
    files = []
    for root, dirs, filenames in os.walk(data_dir):
        for filename in filenames:
            if filename.startswith('class_') and filename.endswith('.txt'):
                filepath = os.path.join(root, filename)
                files.append(filepath)
    return sorted(files)

def main():
    print("=" * 60)
    print("数据清洗工具")
    print("=" * 60)
    print(f"配置:")
    print(f"  - 屏蔽机构: {blocked_orgs}")
    print(f"  - 匹配模式: {blocked_match_mode}")
    print(f"  - 去重策略: {dedup_keep_strategy}")
    print(f"  - 写入前确认: {confirm_before_write}")
    print()
    
    # Step 1: Collect files
    print("步骤 1: 收集文件...")
    files = collect_files()
    print(f"  找到 {len(files)} 个文件")
    print()
    
    # Step 2: First pass - build global mapping
    print("步骤 2: 第一轮遍历 - 构建全局去重映射...")
    
    # Statistics
    total_lines = 0
    blocked_deleted = 0
    unparseable_lines = 0
    unparseable_warnings = []
    
    # Global mapping: (org_name, class_name) -> list of records
    seen = defaultdict(list)
    
    # File content tracking
    file_contents = {}  # filepath -> list of (line, line_num, parse_result)
    
    for file_idx, filepath in enumerate(files):
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        file_records = []
        for line_num, line in enumerate(lines, 1):
            total_lines += 1
            id_str, url, org_name, class_name, parse_status = parse_line(line)
            
            record = {
                'line': line,
                'line_num': line_num,
                'id_str': id_str,
                'url': url,
                'org_name': org_name,
                'class_name': class_name,
                'parse_status': parse_status,
                'id_num': int(id_str) if id_str and id_str.isdigit() else None,
                'file_idx': file_idx,
                'filepath': filepath
            }
            
            file_records.append(record)
            
            if parse_status == "empty" or parse_status == "comment":
                # These are kept as-is
                pass
            elif parse_status == "unparseable":
                unparseable_lines += 1
                unparseable_warnings.append(f"{filepath}:{line_num} - {line.strip()}")
            elif parse_status == "parsed":
                # Check if blocked
                if is_blocked_org(org_name):
                    blocked_deleted += 1
                    record['blocked'] = True
                else:
                    record['blocked'] = False
                    # Add to global mapping
                    key = (org_name, class_name)
                    seen[key].append(record)
        
        file_contents[filepath] = file_records
    
    print(f"  总行数: {total_lines}")
    print(f"  屏蔽机构删除: {blocked_deleted}")
    print(f"  无法解析: {unparseable_lines}")
    print(f"  唯一(机构,班级)组合: {len(seen)}")
    print()
    
    # Step 3: Apply deduplication strategy
    print("步骤 3: 应用去重策略...")
    
    # Select which records to keep for each (org, class) combination
    kept_records = set()  # Set of (filepath, line_num) tuples
    
    dedup_deleted = 0
    
    for key, records in seen.items():
        if len(records) == 1:
            # Only one record, keep it
            kept_records.add((records[0]['filepath'], records[0]['line_num']))
        else:
            # Multiple records, apply strategy
            if dedup_keep_strategy == "first":
                # Keep first occurrence (by file order + line number)
                sorted_records = sorted(records, key=lambda x: (x['file_idx'], x['line_num']))
                kept = sorted_records[0]
            elif dedup_keep_strategy == "last":
                # Keep last occurrence
                sorted_records = sorted(records, key=lambda x: (x['file_idx'], x['line_num']))
                kept = sorted_records[-1]
            elif dedup_keep_strategy == "max_id":
                # Keep record with max ID
                valid_id_records = [r for r in records if r['id_num'] is not None]
                if valid_id_records:
                    kept = max(valid_id_records, key=lambda x: x['id_num'])
                else:
                    # Fallback to first
                    sorted_records = sorted(records, key=lambda x: (x['file_idx'], x['line_num']))
                    kept = sorted_records[0]
            elif dedup_keep_strategy == "min_id":
                # Keep record with min ID
                valid_id_records = [r for r in records if r['id_num'] is not None]
                if valid_id_records:
                    kept = min(valid_id_records, key=lambda x: x['id_num'])
                else:
                    # Fallback to first
                    sorted_records = sorted(records, key=lambda x: (x['file_idx'], x['line_num']))
                    kept = sorted_records[0]
            else:
                # Default to first
                sorted_records = sorted(records, key=lambda x: (x['file_idx'], x['line_num']))
                kept = sorted_records[0]
            
            kept_records.add((kept['filepath'], kept['line_num']))
            dedup_deleted += len(records) - 1
    
    print(f"  去重删除: {dedup_deleted}")
    print()
    
    # Step 4: Show impact summary
    print("步骤 4: 影响范围摘要")
    print("=" * 60)
    
    files_to_modify = []
    total_kept = 0
    
    for filepath in files:
        records = file_contents[filepath]
        original_count = len(records)
        keep_count = 0
        
        for record in records:
            if record['parse_status'] in ["empty", "comment", "unparseable"]:
                # Always keep these
                keep_count += 1
            elif record['parse_status'] == "parsed":
                if record['blocked']:
                    # Blocked, don't keep
                    pass
                else:
                    # Check if kept by deduplication
                    if (filepath, record['line_num']) in kept_records:
                        keep_count += 1
        
        total_kept += keep_count
        if keep_count != original_count:
            files_to_modify.append((filepath, original_count, keep_count))
    
    print(f"将修改的文件数: {len(files_to_modify)}")
    print(f"原始总行数: {total_lines}")
    print(f"屏蔽机构删除: {blocked_deleted}")
    print(f"去重删除: {dedup_deleted}")
    print(f"无法解析保留: {unparseable_lines}")
    print(f"最终保留行数: {total_kept}")
    print()
    
    if files_to_modify:
        print("将修改的文件:")
        for filepath, orig, keep in files_to_modify[:10]:  # Show first 10
            print(f"  {filepath}: {orig} -> {keep} (删除 {orig - keep})")
        if len(files_to_modify) > 10:
            print(f"  ... 还有 {len(files_to_modify) - 10} 个文件")
        print()
    
    if unparseable_warnings:
        print(f"无法解析的行 (共 {unparseable_lines} 行):")
        for warning in unparseable_warnings[:5]:  # Show first 5
            print(f"  {warning}")
        if len(unparseable_warnings) > 5:
            print(f"  ... 还有 {len(unparseable_warnings) - 5} 行")
        print()
    
    # Step 5: Confirm before write
    if confirm_before_write:
        print("=" * 60)
        response = input("请输入 '确认' 以执行写入操作，或按 Ctrl+C 取消: ")
        if response != '确认':
            print("操作已取消")
            return
    
    # Step 6: Write back to files
    print("步骤 5: 写入文件...")
    
    for filepath in files:
        records = file_contents[filepath]
        output_lines = []
        
        for record in records:
            if record['parse_status'] in ["empty", "comment", "unparseable"]:
                # Always keep these as-is
                output_lines.append(record['line'])
            elif record['parse_status'] == "parsed":
                if record['blocked']:
                    # Blocked, skip
                    pass
                else:
                    # Check if kept by deduplication
                    if (filepath, record['line_num']) in kept_records:
                        output_lines.append(record['line'])
        
        # Write back to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(output_lines)
    
    print(f"  已写入 {len(files)} 个文件")
    print()
    
    # Step 7: Final statistics
    print("步骤 6: 统计报告")
    print("=" * 60)
    print(f"处理的文件总数: {len(files)}")
    print(f"读取的总行数: {total_lines}")
    print(f"机构黑名单删除的行数: {blocked_deleted}")
    print(f"重复班级删除的行数: {dedup_deleted}")
    print(f"无法解析的行数: {unparseable_lines}")
    print(f"最终保留的行数: {total_kept}")
    
    # Find backup directory
    backup_dirs = [d for d in os.listdir(r"d:\99\5\1") if d.startswith("data_backup_")]
    if backup_dirs:
        latest_backup = sorted(backup_dirs)[-1]
        print(f"备份路径: d:\\99\\5\\1\\{latest_backup}")
    
    print("=" * 60)
    print("清洗完成!")

if __name__ == "__main__":
    main()
