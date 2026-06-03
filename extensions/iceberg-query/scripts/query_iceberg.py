#!/usr/bin/env python3
"""
Iceberg 查询脚本 — OpenClaw 工具通过子进程调用

支持三种模式：
  --list            列出所有表及 schema
  --describe TABLE  展示表结构 + 采样 + 基数
  --query TABLE     条件查询 (支持 --where, --order-by, --columns, --limit)
  --aggregate TABLE 聚合查询 (支持 --agg-func, --agg-column, --group-by, --where)
"""
import json, os, argparse
from typing import Optional

# AWS credentials (可通过环境变量覆盖)
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "admin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "password")

# Iceberg catalog 配置
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_PATH_STYLE = os.getenv("S3_PATH_STYLE_ACCESS", "true")


def get_catalog():
    from pyiceberg.catalog import load_catalog
    return load_catalog("nd", **{
        "type": "rest",
        "uri": ICEBERG_CATALOG_URI,
        "warehouse": ICEBERG_WAREHOUSE,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.path-style-access": S3_PATH_STYLE,
    })


def list_tables():
    """列出 Iceberg catalog 中所有表及其 schema"""
    catalog = get_catalog()
    result = {}
    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        ns_name = _namespace_to_str(ns)
        if not ns_name:
            continue
        try:
            tables = catalog.list_tables(ns_name)
        except Exception:
            tables = []
        for full_name in tables:
            full_name_str = _table_name_to_str(full_name)
            short = full_name_str.split(".")[-1] if "." in full_name_str else full_name_str
            try:
                t = catalog.load_table(full_name_str)
                schema = t.schema()
                fields = [{"name": f.name, "type": str(f.field_type), "required": f.required}
                          for f in schema.fields]
                # 尝试获取行数估算
                row_count = None
                try:
                    meta = t.metadata
                    if hasattr(meta, 'current_snapshot') and meta.current_snapshot:
                        row_count = meta.current_snapshot.summary.get('total-records')
                        if row_count:
                            row_count = int(row_count)
                except Exception:
                    pass
                result[short] = {
                    "full_name": full_name_str,
                    "columns": fields,
                    "row_count": row_count,
                }
            except Exception as e:
                result[short] = {
                    "full_name": full_name_str,
                    "error": str(e),
                }
    return {"tables": result}


def describe_table(table_name: str):
    """展示表结构 + 采样数据 + 列基数"""
    catalog = get_catalog()

    # 查找表
    full = _resolve_table_name(catalog, table_name)
    table = catalog.load_table(full)
    schema = table.schema()

    fields = [{"name": f.name, "type": str(f.field_type), "required": f.required, "doc": f.doc or ""}
              for f in schema.fields]

    # 采样 3 行
    col_names = [f.name for f in schema.fields]
    arrow = table.scan(limit=3).to_arrow()
    sample_rows = _arrow_to_rows(arrow, col_names, limit=3)

    # 列基数（低基数列的 distinct 值）
    cardinality = {}
    for f in schema.fields:
        # 只对看起来低基数的列检查
        if f.name in ("created_at", "updated_at", "snapshot_time", "timestamp"):
            continue
        try:
            # 用简单聚合方式
            agg_arrow = table.scan(
                selected_fields=(f.name,),
            ).to_arrow()
            vals = set()
            for i in range(min(agg_arrow.num_rows, 10000)):
                v = agg_arrow.column(f.name)[i].as_py()
                if v is not None:
                    if hasattr(v, "isoformat"):
                        v = v.isoformat()
                    elif isinstance(v, bytes):
                        v = v.decode("utf-8", errors="replace")
                    vals.add(str(v))
                if len(vals) > 20:
                    break
            if len(vals) <= 20:
                cardinality[f.name] = {"distinct": len(vals), "values": sorted(vals)}
            else:
                cardinality[f.name] = {"distinct": f"> {len(vals)}", "values": None}
        except Exception:
            cardinality[f.name] = {"distinct": "unknown", "values": None}

    return {
        "table": table_name,
        "full_name": full,
        "columns": fields,
        "sample": sample_rows,
        "cardinality": cardinality,
    }


def query_table(table_name: str, columns: Optional[str], where: Optional[str],
                order_by: Optional[str], limit: int):
    """条件查询，支持 where 过滤"""
    from pyiceberg.expressions import (
        And, EqualTo, NotEqualTo, GreaterThan, LessThan,
        GreaterThanOrEqual, LessThanOrEqual,
        In, NotIn, IsNull, NotNull, StartsWith,
    )

    catalog = get_catalog()
    full = _resolve_table_name(catalog, table_name)
    table = catalog.load_table(full)

    # 构建过滤条件
    filters = []
    if where:
        filters = _parse_where(where)

    # 构建扫描
    selected = tuple(columns.split(",")) if columns else ("*",)
    scan_kwargs = {"limit": limit}
    if selected != ("*",):
        scan_kwargs["selected_fields"] = selected
    if filters:
        scan_kwargs["row_filter"] = And(*filters) if len(filters) > 1 else filters[0]
    scan = table.scan(**scan_kwargs)

    arrow = scan.to_arrow()
    col_names = arrow.schema.names
    rows = _arrow_to_rows(arrow, col_names, limit=limit)

    result = {
        "success": True,
        "table": table_name,
        "columns": col_names,
        "total_scanned": arrow.num_rows,
        "returned_rows": len(rows),
        "rows": rows,
    }

    # 如果指定了 order_by，在内存中排序
    if order_by and rows:
        desc = order_by.startswith("-")
        sort_col = order_by.lstrip("-")
        if sort_col in col_names:
            rows.sort(key=lambda r: (r.get(sort_col) is None, r.get(sort_col, "")), reverse=desc)
            result["rows"] = rows

    return result


def aggregate_table(table_name: str, agg_func: str, agg_column: Optional[str],
                    group_by: Optional[str], where: Optional[str], limit: int):
    """聚合查询：COUNT/SUM/AVG/MIN/MAX，支持 GROUP BY 和 WHERE"""
    catalog = get_catalog()
    full = _resolve_table_name(catalog, table_name)
    table = catalog.load_table(full)

    # 构建过滤
    filters = []
    if where:
        filters = _parse_where(where)

    # 获取数据并在内存中聚合
    # (Iceberg PyIceberg 目前不原生支持 SQL 聚合，需要在内存中完成)
    scan_kwargs = {}
    if filters:
        from pyiceberg.expressions import And as _And
        scan_kwargs["row_filter"] = _And(*filters) if len(filters) > 1 else filters[0]
    scan = table.scan(**scan_kwargs)

    # 如果有 group_by，只选择需要的列
    if group_by or agg_column:
        cols = set()
        if group_by:
            cols.add(group_by)
        if agg_column:
            cols.add(agg_column)
        scan_kwargs["selected_fields"] = tuple(cols)
        scan = table.scan(**scan_kwargs)

    arrow = scan.to_arrow()
    col_names = arrow.schema.names
    all_rows = _arrow_to_rows(arrow, col_names, limit=1_000_000)

    # 聚合
    agg_fn = (agg_func or "COUNT").upper()
    if group_by:
        groups = {}
        for row in all_rows:
            key = row.get(group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        result_rows = []
        for key, group_rows in groups.items():
            val = _compute_agg(agg_fn, agg_column, group_rows)
            result_rows.append({group_by: key, f"{agg_fn.lower()}_result": val})

        # 排序
        result_rows.sort(key=lambda r: r.get(f"{agg_fn.lower()}_result", 0) or 0, reverse=True)
        result_rows = result_rows[:limit]
    else:
        val = _compute_agg(agg_fn, agg_column, all_rows)
        result_rows = [{f"{agg_fn.lower()}_result": val}]

    return {
        "success": True,
        "table": table_name,
        "agg_func": agg_fn,
        "agg_column": agg_column,
        "group_by": group_by,
        "total_scanned": len(all_rows),
        "returned_rows": len(result_rows),
        "rows": result_rows,
    }


# ---- 辅助函数 ----

def _namespace_to_str(ns):
    """递归转换各种 namespace 表示到字符串。
    兼容 PyIceberg 各版本的不同返回类型：
    - str → 直接返回
    - tuple/list → 递归展开，用 "." 连接
    - Namespace 对象 → str()
    """
    if isinstance(ns, str):
        return ns
    if isinstance(ns, bytes):
        return ns.decode("utf-8")
    if hasattr(ns, "root") and hasattr(ns, "levels"):
        # pyiceberg Namespace 对象 (Namespace class)
        return ".".join(str(l) for l in (ns.root + ns.levels))
    if isinstance(ns, (list, tuple)):
        parts = []
        for item in ns:
            sub = _namespace_to_str(item)
            if sub:
                parts.append(sub)
        return ".".join(parts) if parts else ""
    return str(ns)


def _table_name_to_str(table_ident):
    """转换表标识为字符串。
    PyIceberg 的部分版本 list_tables() 返回 tuple 列表，
    如 ('network', 'inventory') → 'network.inventory'
    """
    if isinstance(table_ident, str):
        return table_ident
    if isinstance(table_ident, (list, tuple)):
        return ".".join(str(p) for p in table_ident)
    return str(table_ident)


def _resolve_table_name(catalog, table_name):
    """解析表名：先查已知的 namespace，匹配短名。找不到则用第一个 namespace 拼接"""
    # 如果是全限定名，直接用
    if "." in table_name:
        return table_name

    namespaces = catalog.list_namespaces()
    first_ns = None
    for ns in namespaces:
        ns_name = _namespace_to_str(ns)
        if first_ns is None:
            first_ns = ns_name
        if not ns_name:
            continue
        try:
            tables = catalog.list_tables(ns_name)
            for full_name in tables:
                full_name_str = _table_name_to_str(full_name)
                short = full_name_str.split(".")[-1] if "." in full_name_str else full_name_str
                if short == table_name:
                    return full_name_str
        except Exception:
            continue

    # 没找到，用第一个 namespace 拼接短名作为 fallback（如 "nd.inventory"）
    if first_ns:
        return f"{first_ns}.{table_name}"
    return table_name


def _arrow_to_rows(arrow_table, col_names, limit=500):
    """Arrow Table → list[dict]"""
    rows = []
    for i in range(min(arrow_table.num_rows, limit)):
        row = {}
        for col in col_names:
            try:
                val = arrow_table.column(col)[i].as_py()
                if isinstance(val, bytes):
                    val = val.decode("utf-8", errors="replace")
                elif hasattr(val, "isoformat"):
                    val = val.isoformat()
                row[col] = val
            except Exception:
                row[col] = None
        rows.append(row)
    return rows


def _parse_where(where_str):
    """解析 where 条件 JSON 字符串 → Iceberg filter 表达式列表

    格式: JSON 数组，每个元素 {column, op, value}
    示例: '[{"column":"status","op":"=","value":"active"},{"column":"device_id","op":"IN","value":["d1","d2"]}]'
    """
    from pyiceberg.expressions import (
        EqualTo, NotEqualTo, GreaterThan, LessThan,
        GreaterThanOrEqual, LessThanOrEqual,
        In, NotIn, IsNull, NotNull, StartsWith,
    )

    conditions = json.loads(where_str)
    filters = []

    for cond in conditions:
        col = cond["column"]
        op = cond["op"].upper()
        val = cond.get("value")

        if op == "=":
            filters.append(EqualTo(col, val))
        elif op in ("!=", "<>"):
            filters.append(NotEqualTo(col, val))
        elif op == ">":
            filters.append(GreaterThan(col, val))
        elif op == "<":
            filters.append(LessThan(col, val))
        elif op == ">=":
            filters.append(GreaterThanOrEqual(col, val))
        elif op == "<=":
            filters.append(LessThanOrEqual(col, val))
        elif op == "IN":
            filters.append(In(col, val if isinstance(val, list) else [val]))
        elif op == "NOT IN":
            filters.append(NotIn(col, val if isinstance(val, list) else [val]))
        elif op in ("IS NULL", "IS"):
            filters.append(IsNull(col))
        elif op in ("IS NOT NULL", "IS NOT"):
            filters.append(NotNull(col))
        elif op == "STARTS_WITH":
            filters.append(StartsWith(col, val))
        else:
            raise ValueError(f"Unsupported operator: {op}")

    return filters


def _compute_agg(agg_fn, agg_column, rows):
    """在内存中计算聚合值"""
    if agg_fn == "COUNT":
        if agg_column:
            return sum(1 for r in rows if r.get(agg_column) is not None)
        return len(rows)
    elif agg_fn == "SUM":
        return sum(r.get(agg_column, 0) or 0 for r in rows)
    elif agg_fn == "AVG":
        vals = [r.get(agg_column) for r in rows if r.get(agg_column) is not None]
        return sum(vals) / len(vals) if vals else None
    elif agg_fn == "MIN":
        vals = [r.get(agg_column) for r in rows if r.get(agg_column) is not None]
        return min(vals) if vals else None
    elif agg_fn == "MAX":
        vals = [r.get(agg_column) for r in rows if r.get(agg_column) is not None]
        return max(vals) if vals else None
    else:
        raise ValueError(f"Unknown aggregate function: {agg_fn}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Iceberg 查询工具")
    p.add_argument("--list", action="store_true", help="列出所有表和字段")
    p.add_argument("--describe", default=None, metavar="TABLE", help="展示表结构+采样+基数")
    p.add_argument("--query", default=None, metavar="TABLE", help="条件查询")
    p.add_argument("--aggregate", default=None, metavar="TABLE", help="聚合查询")
    p.add_argument("--columns", default=None, help="列名，逗号分隔")
    p.add_argument("--where", default=None, help="WHERE 条件 (JSON 数组)")
    p.add_argument("--order-by", default=None, help="排序字段，前缀 - 为降序")
    p.add_argument("--limit", type=int, default=50, help="最大返回行数")
    p.add_argument("--agg-func", default="COUNT", help="聚合函数: COUNT/SUM/AVG/MIN/MAX")
    p.add_argument("--agg-column", default=None, help="聚合列名")
    p.add_argument("--group-by", default=None, help="GROUP BY 列名")
    args = p.parse_args()

    try:
        if args.list:
            print(json.dumps(list_tables(), ensure_ascii=False, indent=2))
        elif args.describe:
            print(json.dumps(describe_table(args.describe), ensure_ascii=False, indent=2, default=str))
        elif args.query:
            result = query_table(args.query, args.columns, args.where, args.order_by, args.limit)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        elif args.aggregate:
            result = aggregate_table(args.aggregate, args.agg_func, args.agg_column, args.group_by, args.where, args.limit)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            p.error("Specify one of: --list, --describe TABLE, --query TABLE, --aggregate TABLE")
    except Exception as e:
        print(json.dumps({"success": False, "error": str(e)}, ensure_ascii=False))
