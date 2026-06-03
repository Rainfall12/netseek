#!/usr/bin/env python3
"""NA Config Blob Slicer — 从 Iceberg 读 blob，切块 + 折叠 + 搜索，不解析语义。

数据路径: Iceberg REST Catalog → network.config_snapshot → config_content (blob)
用法:
  python3 slicer.py --device SH-CORE-01 --scan
  python3 slicer.py --device SH-CORE-01 --search "CCTV"
  python3 slicer.py --device SH-CORE-01 --get-block 12
  python3 slicer.py --device SH-CORE-01 --snapshot-time "2026-06-03T02:30:00" --scan
"""

import argparse
import json
import os
import re
import sys

# ─── 分隔符正则（跨厂商通用） ───
# Cisco: ! | Juniper: } | PaloAlto: </rule> | 华为: #
BLOCK_SEP = re.compile(
    r'\n\s*(?:!|#|}\s*|</\w+>)\s*\n',
    re.MULTILINE,
)

os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "admin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "password")


# ─── Iceberg 连接 ───
def get_table():
    from pyiceberg.catalog import load_catalog

    catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    s3_path_style = os.getenv("S3_PATH_STYLE_ACCESS", "true")

    catalog = load_catalog("nd", **{
        "type": "rest",
        "uri": catalog_uri,
        "warehouse": warehouse,
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.path-style-access": s3_path_style,
    })
    return catalog.load_table("network.config_snapshot")


# ─── 数据拉取 ───
def get_config(device_id, snapshot_time=None):
    """从 Iceberg config_snapshot 表拉取 config_content blob"""
    from pyiceberg.expressions import EqualTo, And

    table = get_table()

    filter_expr = EqualTo("device_id", device_id)
    if snapshot_time:
        filter_expr = And(
            filter_expr,
            EqualTo("snapshot_time", snapshot_time),
        )

    # 只取需要的列，按 snapshot_time 降序取最新一条
    scan = table.scan(
        row_filter=filter_expr,
        selected_fields=("device_id", "config_content", "snapshot_time"),
    )

    arrow = scan.to_arrow()
    if arrow.num_rows == 0:
        raise ValueError(f"No config found for device '{device_id}'")

    # 取最新一条（在内存中排序）
    rows = []
    col_names = arrow.schema.names
    for i in range(arrow.num_rows):
        row = {col: arrow.column(col)[i].as_py() for col in col_names}
        rows.append(row)

    # 按 snapshot_time 降序，取第一条
    rows.sort(
        key=lambda r: (
            r.get("snapshot_time") is None,
            r.get("snapshot_time"),
        ),
        reverse=True,
    )
    row = rows[0]

    content = row["config_content"]
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")

    updated_at = row.get("snapshot_time")
    if hasattr(updated_at, "isoformat"):
        updated_at = updated_at.isoformat()
    elif updated_at is None:
        updated_at = "unknown"

    return content, updated_at


# ─── 切块 ───
def chunk_config(text):
    """按分隔符切块，返回 [{index, key, title, lines, text}]"""
    raw_blocks = BLOCK_SEP.split(text)
    chunks = []
    idx = 0
    for block in raw_blocks:
        stripped = [l.rstrip() for l in block.strip().split('\n') if l.strip()]
        if not stripped:
            continue
        idx += 1
        title = stripped[0][:80]
        # 提取折叠 key：取开头到第一个空格或数字之前的公共前缀
        fold_key = re.match(r'^(\S+(?:\s+\S+){0,2})', title)
        key = fold_key.group(1) if fold_key else title[:30]
        chunks.append({
            'index': idx,
            'key': key,
            'title': title,
            'lines': len(stripped),
            'text': '\n'.join(stripped),
        })
    return chunks


# ─── 折叠 ───
def fold_overview(chunks, max_rows=20):
    """相邻相同 key 的块自动合并折叠"""
    if not chunks:
        return "_(config is empty)_"

    folded = []
    i = 0
    while i < len(chunks):
        group = [chunks[i]]
        j = i + 1
        while j < len(chunks) and chunks[j]['key'] == group[0]['key']:
            group.append(chunks[j])
            j += 1

        if len(group) == 1:
            c = group[0]
            folded.append(f"  #{c['index']:>3d}  {c['title']:<70s}  {c['lines']}行")
        else:
            first = group[0]
            last = group[-1]
            total_lines = sum(c['lines'] for c in group)
            folded.append(
                f"  #{first['index']:>3d}-"
                f"#{last['index']:<3d}  {first['title']:<70s}  "
                f"({len(group)}块, {total_lines}行)"
            )
        i = j
        if len(folded) >= max_rows:
            folded.append(f"  ... 还有 {len(chunks) - i} 块未显示")
            break

    return '\n'.join(folded)


# ─── 搜索 ───
def search_chunks(chunks, keyword, max_blocks=10, max_lines=300):
    """返回包含关键词的块的完整文本"""
    keyword_lower = keyword.lower()
    matched = [c for c in chunks if keyword_lower in c['text'].lower()]
    if not matched:
        return None, f"(未找到包含 '{keyword}' 的配置块)"

    total_lines = 0
    result_blocks = []
    for c in matched[:max_blocks]:
        block_text = f"# ── 块 #{c['index']} — {c['title']} ({c['lines']}行) ──\n{c['text']}"
        result_blocks.append(block_text)
        total_lines += c['lines']
        if total_lines >= max_lines:
            result_blocks.append(f"\n(已达到 {max_lines} 行上限，共 {len(matched)} 个匹配块)")
            break

    return matched, '\n\n'.join(result_blocks)


# ─── 获取指定块 ───
def get_block(chunks, index):
    for c in chunks:
        if c['index'] == index:
            return c['text']
    return f"(块 #{index} 不存在)"


# ─── CLI ───
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--device', required=True)
    p.add_argument('--snapshot-time', default=None)
    p.add_argument('--scan', action='store_true')
    p.add_argument('--search', default=None)
    p.add_argument('--get-block', type=int, default=None)
    args = p.parse_args()

    try:
        blob, updated_at = get_config(args.device, args.snapshot_time)

        if not blob or not blob.strip():
            print(json.dumps({'success': False, 'error': 'Config blob is empty'}))
            return

        chunks = chunk_config(blob)
        total_lines = len(blob.split('\n'))
        output = {
            'success': True,
            'device_id': args.device,
            'updated_at': updated_at,
            'total_chunks': len(chunks),
            'total_lines': total_lines,
        }

        if args.scan:
            output['overview'] = (
                f"{args.device} — {len(chunks)}块 / {total_lines}行  "
                f"更新时间: {updated_at}\n\n"
                + fold_overview(chunks)
            )

        elif args.get_block:
            output['block'] = get_block(chunks, args.get_block)
            output['block_index'] = args.get_block

        elif args.search:
            match_chunks, text = search_chunks(chunks, args.search)
            output['search'] = args.search
            output['text'] = text
            output['match_count'] = len(match_chunks) if match_chunks else 0

        else:
            # 默认：返回概览
            output['overview'] = (
                f"{args.device} — {len(chunks)}块 / {total_lines}行  "
                f"更新时间: {updated_at}\n\n"
                + fold_overview(chunks)
            )

        print(json.dumps(output, ensure_ascii=False, default=str))

    except Exception as err:
        print(json.dumps({'success': False, 'error': str(err)[:500]}))
        sys.exit(1)


if __name__ == '__main__':
    main()
