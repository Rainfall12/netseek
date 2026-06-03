import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Type } from "typebox";

const execFileAsync = promisify(execFile);

// ─── PostgreSQL 连接参数 ───
const PG_HOST = process.env.PGHOST || "localhost";
const PG_PORT = process.env.PGPORT || "5432";
const PG_USER = process.env.PGUSER || "iceberg";
const PG_PASSWORD = process.env.PGPASSWORD || "iceberg";
const PG_DATABASE = process.env.PGDATABASE || "iceberg";

// ─── SQL 安全白名单 ───
const ALLOWED_OPS = [
  "=",
  "!=",
  "<>",
  ">",
  "<",
  ">=",
  "<=",
  "LIKE",
  "NOT LIKE",
  "ILIKE",
  "NOT ILIKE",
  "IN",
  "NOT IN",
  "IS",
  "IS NOT",
  "BETWEEN",
];

// ─── 当前 RCA 运行上下文 ───
let currentLevel = "L0";
let queriedRanges = new Map();
let normalSet = new Set();
const CACHE_TTL_MS = 5 * 60_000;

// ─── 边界常量 ───
const LEVEL_CONFIG = {
  L0: { time_window_min: 15, scope: "device", data_limit: 50, description: "极速：单设备±15分钟" },
  L1: {
    time_window_min: 30,
    scope: "device_group",
    data_limit: 100,
    description: "快速：设备组±30分钟",
  },
  L2: { time_window_min: 60, scope: "room", data_limit: 200, description: "标准：机房±60分钟" },
  L3: { time_window_min: 120, scope: "region", data_limit: 500, description: "深度：区域±120分钟" },
};
const MAX_TIME_WINDOW_MIN = 480;
const MAX_LOG_ROWS = 100;
const MAX_ALERT_ROWS = 50;
const MAX_METRIC_ITEMS = 3;

// ─── 已知表的时序/空间列映射（用于自动绑定 time+space 过滤） ───
const TABLE_TIME_COLUMNS = {
  inventory: null,
  config_snapshot: "backup_time",
  topology: null,
  monitor_config: null,
  metric_ts: "timestamp",
};
const TABLE_SPACE_COLUMNS = {
  inventory: "name",
  config_snapshot: "device_name",
  topology: "source_device",
  monitor_config: "device_name",
  metric_ts: "device_name",
};

// ─── RCA 参数 Schema ───
const QueryPgSqlSchema = Type.Object(
  {
    // ── 核心 RCA 动作 ──
    action: Type.String({
      description:
        "RCA action: " +
        "'discover' — list all tables + their row counts & column schemas (L0+, first call only, cached 10min); " +
        "'check' — quick aggregate scan for anomalies (COUNT/MAX/AVG) within current boundary; " +
        "'aggregate' — targeted aggregate within time+space boundary (use after check finds anomaly); " +
        "'detail' — raw rows with narrow scope (only after aggregate shows anomaly, ≤5 columns); " +
        "'correlate' — cross-table correlation to link findings (e.g. topology→metric, config→event).",
    }),

    // ── 假设追踪 ──
    hypothesis: Type.Optional(
      Type.String({
        description:
          "What hypothesis is this query testing? " +
          "E.g. '端口拥塞导致丢包', '设备A配置变更导致中断'. " +
          "Required for check/aggregate/detail/correlate.",
      }),
    ),

    // ── 时间边界 ──
    fault_time: Type.Optional(
      Type.String({
        description:
          "ISO 8601 timestamp of fault occurrence. " +
          "ALL time queries pivot here. Do NOT use relative times like 'recent 1 hour'.",
      }),
    ),
    time_window_min: Type.Optional(
      Type.Number({
        description:
          "Half-window min: fault_time ± N min. Start 15, double: 15→30→60→120. Max 480.",
        minimum: 5,
        maximum: 480,
      }),
    ),

    // ── 空间边界 ──
    scope: Type.Optional(
      Type.String({
        description:
          "Spatial scope: 'device' | 'device_group' | 'room' | 'region'. No skipping levels.",
      }),
    ),
    scope_target: Type.Optional(
      Type.String({
        description:
          "Specific device/group/room/region name. Use 'discover' first to learn available names.",
      }),
    ),
    exclude_normal: Type.Optional(
      Type.Array(Type.String(), {
        description: "Devices/groups/rooms confirmed normal. Filtered out automatically.",
      }),
    ),

    // ── 数据边界 ──
    severity: Type.Optional(
      Type.String({
        description: "Log severity filter: 'error'|'warn'|'info'|'all'. Default: 'error'.",
      }),
    ),
    aggregate_first: Type.Optional(
      Type.Boolean({
        description: "Force aggregate before raw detail. Always prefer true initially.",
        default: true,
      }),
    ),

    // ── 查询参数 ──
    table: Type.Optional(
      Type.String({
        description:
          "Table name. Required for check/aggregate/detail/correlate. " +
          "Available: inventory, config_snapshot, topology, monitor_config, metric_ts.",
      }),
    ),
    columns: Type.Optional(
      Type.String({
        description:
          "Comma-separated columns. For detail: ≤5 columns, only relevant to hypothesis.",
      }),
    ),
    where: Type.Optional(
      Type.Array(
        Type.Object({
          column: Type.String({ description: "Column name" }),
          op: Type.String({ description: ALLOWED_OPS.join(", ") }),
          value: Type.Any({ description: "Value. Array for IN/BETWEEN." }),
        }),
        { description: "Additional filters beyond auto-added time+space boundary." },
      ),
    ),
    order_by: Type.Optional(Type.String({ description: "Sort column. '-' prefix for DESC." })),
    limit: Type.Optional(
      Type.Number({
        description: "Max rows. Auto-set by level: L0=50, L1=100, L2=200, L3=500.",
        minimum: 1,
        maximum: 500,
      }),
    ),
    agg_func: Type.Optional(
      Type.String({
        description: "Aggregate: COUNT, SUM, AVG, MIN, MAX. Required for aggregate.",
      }),
    ),
    agg_column: Type.Optional(Type.String({ description: "Column to aggregate." })),
    group_by: Type.Optional(Type.String({ description: "Group-by column." })),

    // ── 关系分析 ──
    correlate_with: Type.Optional(
      Type.String({
        description:
          "For 'correlate': second table to join. E.g. correlate topology with metric_ts.",
      }),
    ),
  },
  { additionalProperties: false },
);

// ── 辅助函数 ──
function formatTable(cols, rows, maxCellLen = 200) {
  if (!cols.length || !rows.length) return "_(no data)_";
  const header = `| ${cols.join(" | ")} |`;
  const sep = `| ${cols.map(() => "---").join(" | ")} |`;
  const body = rows
    .map((row) => {
      const vals = cols.map((c) => {
        let v = row[c];
        if (v === null || v === undefined) return "";
        const str = String(v);
        return str.length > maxCellLen ? str.slice(0, maxCellLen - 3) + "..." : str;
      });
      return `| ${vals.join(" | ")} |`;
    })
    .join("\n");
  return `${header}\n${sep}\n${body}`;
}

function getEffectiveLimit(params) {
  return params.limit || LEVEL_CONFIG[currentLevel]?.data_limit || 50;
}

function escapeSqlValue(val) {
  if (val === null || val === undefined) return "NULL";
  if (val === true) return "TRUE";
  if (val === false) return "FALSE";
  if (typeof val === "number") return String(val);
  if (Array.isArray(val)) {
    return "(" + val.map((v) => escapeSqlValue(v)).join(", ") + ")";
  }
  return "'" + String(val).replace(/'/g, "''") + "'";
}

function buildWhereClause(conditions) {
  if (!conditions || conditions.length === 0) return "";
  const clauses = [];
  for (const cond of conditions) {
    const { column, op, value } = cond;
    const upperOp = String(op).toUpperCase();
    if (!ALLOWED_OPS.includes(upperOp)) {
      throw new Error(`Operator "${op}" not allowed.`);
    }
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(column)) {
      throw new Error(`Invalid column: "${column}"`);
    }
    if (upperOp === "BETWEEN") {
      if (!Array.isArray(value) || value.length !== 2) throw new Error("BETWEEN needs 2 values");
      clauses.push(
        `"${column}" BETWEEN ${escapeSqlValue(value[0])} AND ${escapeSqlValue(value[1])}`,
      );
    } else if (upperOp === "IN" || upperOp === "NOT IN") {
      if (!Array.isArray(value)) throw new Error(`${upperOp} needs array`);
      clauses.push(`"${column}" ${upperOp} ${escapeSqlValue(value)}`);
    } else if (upperOp === "IS" || upperOp === "IS NOT") {
      clauses.push(
        `"${column}" ${upperOp} ${value === null ? "NULL" : String(value).toUpperCase()}`,
      );
    } else {
      clauses.push(`"${column}" ${upperOp} ${escapeSqlValue(value)}`);
    }
  }
  return "WHERE " + clauses.join(" AND ");
}

function buildOrderBy(orderBy) {
  if (!orderBy) return "";
  const desc = orderBy.startsWith("-");
  const col = desc ? orderBy.slice(1) : orderBy;
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col)) throw new Error(`Invalid order_by: "${col}"`);
  return `ORDER BY "${col}" ${desc ? "DESC" : "ASC"}`;
}

function buildTimeFilter(params) {
  if (!params.fault_time || !params.table) return "";
  const timeCol = TABLE_TIME_COLUMNS[params.table];
  if (!timeCol) return "";
  const windowMin = params.time_window_min || LEVEL_CONFIG[currentLevel]?.time_window_min || 15;
  try {
    const ft = new Date(params.fault_time);
    const start = new Date(ft.getTime() - windowMin * 60_000);
    const end = new Date(ft.getTime() + windowMin * 60_000);
    return `"${timeCol}" BETWEEN ${escapeSqlValue(start.toISOString())} AND ${escapeSqlValue(end.toISOString())}`;
  } catch {
    return "";
  }
}

function buildSpaceFilter(params) {
  if (!params.scope_target || !params.table) return "";
  const spaceCol = TABLE_SPACE_COLUMNS[params.table];
  if (!spaceCol) return "";
  if (params.scope === "region") {
    // 区域级：LIKE 匹配或 IN
    return `"${spaceCol}" LIKE ${escapeSqlValue(params.scope_target + "%")}`;
  }
  return `"${spaceCol}" = ${escapeSqlValue(params.scope_target)}`;
}

function buildExcludeFilter(params) {
  if (!params.exclude_normal?.length || !params.table) return "";
  const spaceCol = TABLE_SPACE_COLUMNS[params.table];
  if (!spaceCol) return "";
  const list = params.exclude_normal.map((n) => escapeSqlValue(n)).join(", ");
  return `"${spaceCol}" NOT IN (${list})`;
}

function buildAutoWhere(params) {
  const parts = [];
  const tf = buildTimeFilter(params);
  if (tf) parts.push(tf);
  const sf = buildSpaceFilter(params);
  if (sf) parts.push(sf);
  const ef = buildExcludeFilter(params);
  if (ef) parts.push(ef);
  if (params.where) {
    parts.push(buildWhereClause(params.where).replace(/^WHERE\s+/, ""));
  }
  if (params.severity && params.severity !== "all") {
    const sevVal = escapeSqlValue(params.severity.toUpperCase());
    parts.push(`"severity" = ${sevVal}`);
  }
  return parts.length > 0 ? "WHERE " + parts.join(" AND ") : "";
}

function buildSelectClause(params) {
  const cols = params.columns ? params.columns.split(",").map((c) => `"${c.trim()}"`) : ["*"];
  return cols.join(", ");
}

// ── psql 执行 ──
async function runPsqlQuery(sql, useDocker = false) {
  if (useDocker) {
    const { stdout } = await execFileAsync(
      "docker",
      [
        "exec",
        "-e",
        `PGPASSWORD=${PG_PASSWORD}`,
        "iceberg-postgres",
        "psql",
        "-h",
        "localhost",
        "-U",
        PG_USER,
        "-d",
        PG_DATABASE,
        "-t",
        "-A",
        "-F",
        "|",
        "-c",
        sql,
      ],
      { timeout: 30_000, maxBuffer: 5 * 1024 * 1024 },
    );
    return stdout;
  }
  const env = { ...process.env, PGPASSWORD: PG_PASSWORD };
  const { stdout } = await execFileAsync(
    "psql",
    [
      "-h",
      PG_HOST,
      "-p",
      PG_PORT,
      "-U",
      PG_USER,
      "-d",
      PG_DATABASE,
      "-t",
      "-A",
      "-F",
      "|",
      "-c",
      sql,
    ],
    { env, timeout: 30_000, maxBuffer: 5 * 1024 * 1024 },
  );
  return stdout;
}

async function safeRunPsql(sql, tableName) {
  try {
    return await runPsqlQuery(sql, false);
  } catch (directErr) {
    try {
      return await runPsqlQuery(sql, true);
    } catch (dockerErr) {
      const msg = dockerErr instanceof Error ? dockerErr.message : String(dockerErr);

      // 检测 "relation does not exist" — 数据在 Iceberg 中
      if (tableName && /relation.*does not exist/i.test(msg)) {
        throw new Error(
          `Table "${tableName}" does not exist in PostgreSQL. ` +
            `Its data is stored in Apache Iceberg (MinIO + REST catalog). ` +
            `Use the **query_iceberg** tool instead.`,
        );
      }

      throw new Error(`psql failed (both direct & docker): ${msg}`);
    }
  }
}

function parsePsqlOutput(output, columns) {
  const lines = output
    .trim()
    .split("\n")
    .filter((l) => l.trim() !== "");
  const rows = [];
  for (const line of lines) {
    const values = [];
    let current = "";
    let inJson = 0;
    for (const ch of line) {
      if (ch === "{" || ch === "[") inJson++;
      if (ch === "}" || ch === "]") inJson--;
      if (ch === "|" && inJson === 0) {
        values.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
    values.push(current);
    const row = {};
    for (let i = 0; i < columns.length; i++) {
      const val = values[i] !== undefined ? values[i] : "";
      row[columns[i]] = val === "\\N" ? null : val;
    }
    rows.push(row);
  }
  return rows;
}

function formatBoundaryState(params, anomalyDetected) {
  const lvl = LEVEL_CONFIG[currentLevel];
  return {
    level: currentLevel,
    level_desc: lvl?.description || "unknown",
    time_window: params.time_window_min ? `±${params.time_window_min}min` : "none",
    scope: params.scope || "unspecified",
    scope_target: params.scope_target || "unspecified",
    anomaly_detected: anomalyDetected,
  };
}

function formatRecommendation(anomalyDetected, hasData) {
  if (anomalyDetected) {
    return "🔍 **Anomaly found!** → Narrow scope. Use `action='detail'` for raw evidence with minimal columns.";
  }
  if (!hasData) {
    const next = Math.min(
      (LEVEL_CONFIG[currentLevel]?.time_window_min || 15) * 2,
      MAX_TIME_WINDOW_MIN,
    );
    return `🔄 No anomaly → Expand time_window to ±${next}min or broaden scope to next spatial level.`;
  }
  return "✅ Query complete. Review and update hypothesis.";
}

function checkCache(key) {
  const last = queriedRanges.get(key);
  return last && Date.now() - last < CACHE_TTL_MS;
}
function markCache(key) {
  queriedRanges.set(key, Date.now());
}

// ── RCA Actions ──

async function actionDiscover() {
  const sql = `
    SELECT t.table_name,
           COALESCE(s.n_live_tup, 0) AS row_count,
           (SELECT string_agg(column_name || ' (' || data_type || ')', ', ' ORDER BY ordinal_position)
            FROM information_schema.columns c WHERE c.table_name = t.table_name) AS columns
    FROM information_schema.tables t
    LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
    WHERE t.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
      AND t.table_type = 'BASE TABLE'
    ORDER BY t.table_name;
  `;
  const stdout = await safeRunPsql(sql);
  const rows = parsePsqlOutput(stdout, ["table_name", "row_count", "columns"]);

  // 验证每张表是否真正可在 PgSQL 中查询（部分表数据只存在于 Iceberg/MinIO 中）
  const pgsqlTables = [];
  const icebergTables = [];

  for (const r of rows) {
    try {
      await safeRunPsql(`SELECT 1 FROM "${r.table_name}" LIMIT 1;`);
      pgsqlTables.push(r);
    } catch {
      // 表不可查询 — 数据在 Iceberg 中，PgSQL 只有元数据
      icebergTables.push(r);
    }
  }

  let text = "";

  if (pgsqlTables.length > 0) {
    const pgsqlLines = pgsqlTables.map(
      (r) => `- **${r.table_name}** (~${r.row_count} rows) | ${r.columns || "n/a"}`,
    );
    text += `## 📊 PostgreSQL Tables (queryable here)\n\n${pgsqlLines.join("\n")}\n\n`;
  }

  if (icebergTables.length > 0) {
    const icebergLines = icebergTables.map(
      (r) => `- **${r.table_name}** (~${r.row_count} rows) | ${r.columns || "n/a"}`,
    );
    text +=
      `## 🧊 Iceberg Tables → use \`query_iceberg\` tool\n\n${icebergLines.join("\n")}\n\n` +
      `⚠️ 这些表的**数据**存储在 Apache Iceberg (MinIO + REST catalog) 中，` +
      `PostgreSQL 里仅有元数据。必须使用 **query_iceberg** 工具查询。\n\n`;
  }

  text +=
    `### RCA Usage\n` +
    `| Table | Purpose | Tool | Key Columns |\n|---|---|---|---|\n` +
    `| inventory | Device list | query_iceberg | device_id, hostname, location, status |\n` +
    `| config_snapshot | Config changes | query_iceberg | device_id, snapshot_time, config_type |\n` +
    `| topology | Links/topology | query_iceberg | source_device_id, target_device_id, status |\n` +
    `| monitor_config | Metric configs | query_iceberg | device_id, metric_name, alert_threshold |\n` +
    `| metric_ts | Time-series metrics | query_iceberg | device_id, metric_name, timestamp, value |\n` +
    `| model_calls | AI model calls | query_pgsql | call_time, latency_ms, total_tokens |\n` +
    `| sessions | Chat sessions | query_pgsql | started_at, session_id, model_id |\n` +
    `| tool_calls | Tool invocations | query_pgsql | call_time, tool_name, status |\n\n` +
    `**Next**: For network RCA (inventory/metric_ts/topology) → use **query_iceberg** with \`action='check'\`. ` +
    `For operational data (sessions/tool_calls) → use this tool.`;

  return {
    content: [{ type: "text", text }],
    details: {
      pgsql_tables: pgsqlTables,
      iceberg_tables: icebergTables,
      boundary_state: formatBoundaryState({}, false),
    },
  };
}

async function actionCheck(params) {
  if (!params.hypothesis) {
    return { content: [{ type: "text", text: "❌ `hypothesis` required for check." }] };
  }
  if (!params.fault_time) {
    return { content: [{ type: "text", text: "❌ `fault_time` required." }] };
  }

  const table = params.table || "metric_ts";
  const effLimit = Math.min(getEffectiveLimit(params), MAX_METRIC_ITEMS * 5);
  const whereClause = buildAutoWhere(params);
  const orderClause = buildOrderBy(params.order_by);

  const sql = `SELECT ${buildSelectClause(params)} FROM "${table}" ${whereClause} ${orderClause} LIMIT ${effLimit};`;
  const stdout = await safeRunPsql(sql, table);

  const cols = params.columns ? params.columns.split(",").map((c) => c.trim()) : ["*"];
  const rows = parsePsqlOutput(stdout, cols === ["*"] ? [] : cols);

  const hasData = rows.length > 0;
  let anomalyDetected = false;
  if (hasData) {
    for (const row of rows.slice(0, 5)) {
      for (const [k, v] of Object.entries(row)) {
        if (k.match(/error|drop|loss|fail|exceed/i) && v && String(v) !== "0") {
          anomalyDetected = true;
          break;
        }
      }
    }
  }

  const boundary = formatBoundaryState(params, anomalyDetected);
  const rec = formatRecommendation(anomalyDetected, hasData);

  const text =
    `## 🔎 RCA Check\n\n` +
    `**Hypothesis**: ${params.hypothesis}\n` +
    `**Table**: ${table} | **Level**: ${boundary.level_desc}\n` +
    `**Time**: ${params.fault_time} ±${boundary.time_window}\n` +
    `**Scope**: ${boundary.scope_target} | **Excluded**: ${normalSet.size} items\n\n` +
    `**Result**: ${rows.length} rows | Anomaly: ${anomalyDetected ? "⚠️ YES" : "✅ none"}\n\n` +
    formatTable(Object.keys(rows[0] || {}), rows) +
    `\n\n` +
    rec;

  return {
    content: [{ type: "text", text }],
    details: { sql, rows, boundary_state: boundary, recommendation: rec },
  };
}

async function actionAggregate(params) {
  if (!params.table) return { content: [{ type: "text", text: "❌ `table` required." }] };

  const aggFunc = (params.agg_func || "COUNT").toUpperCase();
  const validFuncs = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
  if (!validFuncs.includes(aggFunc)) {
    return { content: [{ type: "text", text: `❌ Invalid agg_func: ${aggFunc}` }] };
  }

  const aggExpr =
    aggFunc === "COUNT" && !params.agg_column ? "COUNT(*)" : `${aggFunc}("${params.agg_column}")`;
  const resultCol = `${aggFunc.toLowerCase()}_result`;
  const groupClause = params.group_by ? `, "${params.group_by}"` : "";
  const groupByClause = params.group_by ? `GROUP BY "${params.group_by}"` : "";
  const selectCols = params.group_by
    ? `"${params.group_by}", ${aggExpr} AS ${resultCol}`
    : `${aggExpr} AS ${resultCol}`;
  const whereClause = buildAutoWhere(params);
  const safeLimit = getEffectiveLimit(params);

  const sql = `SELECT ${selectCols} FROM "${params.table}" ${whereClause} ${groupByClause} ORDER BY ${resultCol} DESC LIMIT ${safeLimit};`;
  const stdout = await safeRunPsql(sql, params.table);

  const outCols = params.group_by ? [params.group_by, resultCol] : [resultCol];
  const rows = parsePsqlOutput(stdout, outCols);

  const text =
    `## 📈 Aggregate: ${aggFunc} on ${params.table}` +
    (params.agg_column ? `.${params.agg_column}` : "") +
    (params.group_by ? ` by ${params.group_by}` : "") +
    (params.hypothesis ? `\n**Hypothesis**: ${params.hypothesis}` : "") +
    `\n\n${formatTable(outCols, rows)}`;

  return {
    content: [{ type: "text", text }],
    details: { sql, rows, boundary_state: formatBoundaryState(params, rows.length > 0) },
  };
}

async function actionDetail(params) {
  if (!params.table) return { content: [{ type: "text", text: "❌ `table` required." }] };

  const whereClause = buildAutoWhere(params);
  const orderClause = buildOrderBy(params.order_by);
  const safeLimit = Math.min(getEffectiveLimit(params), MAX_LOG_ROWS);

  // 自动获取列名
  const metaSql = `SELECT column_name FROM information_schema.columns WHERE table_name = '${params.table.replace(/'/g, "''")}' ORDER BY ordinal_position;`;
  const metaOut = await safeRunPsql(metaSql);
  const allCols = parsePsqlOutput(metaOut, ["column_name"]).map((r) => r.column_name);

  const selectCols = params.columns
    ? params.columns
        .split(",")
        .map((c) => `"${c.trim()}"`)
        .join(", ")
    : allCols.map((c) => `"${c}"`).join(", ");
  const resultCols = params.columns ? params.columns.split(",").map((c) => c.trim()) : allCols;

  const sql = `SELECT ${selectCols} FROM "${params.table}" ${whereClause} ${orderClause} LIMIT ${safeLimit};`;
  const stdout = await safeRunPsql(sql, params.table);
  const rows = parsePsqlOutput(stdout, resultCols);

  const text =
    `## 📋 Detail: ${params.table}` +
    (params.hypothesis ? `\n**Hypothesis**: ${params.hypothesis}` : "") +
    `\n${rows.length} row(s)\n\n` +
    formatTable(resultCols, rows);

  return {
    content: [{ type: "text", text }],
    details: { sql, rows, boundary_state: formatBoundaryState(params, rows.length > 0) },
  };
}

async function actionCorrelate(params) {
  if (!params.table || !params.correlate_with) {
    return {
      content: [{ type: "text", text: "❌ `table` and `correlate_with` required." }],
    };
  }

  const safeLimit = Math.min(getEffectiveLimit(params), 30);
  const whereClause = buildAutoWhere(params);

  // 主表
  const pSql = `SELECT * FROM "${params.table}" ${whereClause} LIMIT ${safeLimit};`;
  const pOut = await safeRunPsql(pSql, params.table);
  const pMeta = `SELECT column_name FROM information_schema.columns WHERE table_name = '${params.table.replace(/'/g, "''")}' ORDER BY ordinal_position;`;
  const pMetaOut = await safeRunPsql(pMeta);
  const pCols = parsePsqlOutput(pMetaOut, ["column_name"]).map((r) => r.column_name);
  const pRows = parsePsqlOutput(pOut, pCols);

  // 关联表
  const sSql = `SELECT * FROM "${params.correlate_with}" ${whereClause} LIMIT ${safeLimit};`;
  const sOut = await safeRunPsql(sSql, params.correlate_with);
  const sMeta = `SELECT column_name FROM information_schema.columns WHERE table_name = '${params.correlate_with.replace(/'/g, "''")}' ORDER BY ordinal_position;`;
  const sMetaOut = await safeRunPsql(sMeta);
  const sCols = parsePsqlOutput(sMetaOut, ["column_name"]).map((r) => r.column_name);
  const sRows = parsePsqlOutput(sOut, sCols);

  const text =
    `## 🔗 Correlate: ${params.table} ↔ ${params.correlate_with}\n` +
    (params.hypothesis ? `**Hypothesis**: ${params.hypothesis}\n\n` : "\n") +
    `### ${params.table}\n${formatTable(pCols, pRows)}\n\n` +
    `### ${params.correlate_with}\n${formatTable(sCols, sRows)}`;

  return { content: [{ type: "text", text }], details: { primary: pRows, secondary: sRows } };
}

async function actionDescribe(tableName) {
  const colSql = `SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = '${tableName.replace(/'/g, "''")}' ORDER BY ordinal_position;`;
  const colOut = await safeRunPsql(colSql, tableName);
  const colRows = parsePsqlOutput(colOut, [
    "column_name",
    "data_type",
    "is_nullable",
    "column_default",
  ]);

  const countSql = `SELECT COUNT(*) FROM "${tableName}";`;
  const countOut = await safeRunPsql(countSql, tableName);
  const countRows = parsePsqlOutput(countOut, ["count"]);
  const rowCount = countRows[0]?.count || "0";

  const sampleSql = `SELECT * FROM "${tableName}" LIMIT 3;`;
  const sampleOut = await safeRunPsql(sampleSql, tableName);
  const colNames = colRows.map((c) => c.column_name);
  const sampleRows = parsePsqlOutput(sampleOut, colNames);

  const schemaLines = colRows.map(
    (c) =>
      `  - \`${c.column_name}\` ${c.data_type}${c.is_nullable === "YES" ? ", nullable" : ""}${c.column_default ? `, default: ${c.column_default}` : ""}`,
  );

  const text =
    `## Table: ${tableName} (~${rowCount} rows)\n\n` +
    `### Columns\n${schemaLines.join("\n")}\n\n` +
    `### Sample (3 rows)\n${formatTable(colNames, sampleRows)}`;

  return { content: [{ type: "text", text }], details: { table: tableName, row_count: rowCount } };
}

// ── 工具定义 ──
export function createQueryPgSqlTool() {
  return {
    name: "query_pgsql",
    label: "Query PostgreSQL (RCA)",
    description:
      "Query network device data from PostgreSQL with built-in RCA efficiency boundaries.\n\n" +
      "### RCA Workflow (follow strictly):\n" +
      "1. `action='discover'` → learn available tables & spatial scopes (once, cached 10min)\n" +
      "2. `action='check'` hypothesis='...' fault_time='...' → quick anomaly scan within current boundary\n" +
      "3. If anomaly found → `action='aggregate'` to quantify, then `action='detail'` for evidence\n" +
      "4. If no anomaly → expand time_window or scope, re-run `action='check'`\n" +
      "5. `action='correlate'` → cross-table analysis only after finding specific clues\n\n" +
      "### Boundary Rules (automatic):\n" +
      "- Time: center on `fault_time`, start ±15min, double on no-finding (max ±8h)\n" +
      "- Space: device → device_group → room → region (no skipping)\n" +
      "- Data: aggregate first, detail only on anomaly, error-only logs, ≤3 metric items\n\n" +
      "### Anti-patterns (NEVER do):\n" +
      "❌ Query without fault_time or hypothesis\n" +
      "❌ SELECT * on large tables without narrow WHERE\n" +
      "❌ Skip aggregate and go straight to raw detail\n" +
      "❌ Use 'recent 1 hour' instead of fault_time ± offset\n" +
      "❌ Query already-verified-normal devices (maintain exclude_normal list)\n\n" +
      "### Performance: pgsql is faster for simple queries; iceberg is better for large scans.\n" +
      "Prefer pgsql for check/aggregate/detail. Use iceberg for full-table scans or complex filters.",
    parameters: QueryPgSqlSchema,

    execute: async (_toolCallId, rawParams) => {
      const params = rawParams;

      const cacheKey = JSON.stringify({ action: params.action, table: params.table });
      if (params.action === "discover" && checkCache(cacheKey)) {
        return {
          content: [
            {
              type: "text",
              text: "📌 Table list cached (<5min). Use `action='check'` to proceed.",
            },
          ],
          details: { cached: true },
        };
      }

      try {
        let result;
        switch (params.action) {
          case "discover":
            markCache(cacheKey);
            result = await actionDiscover();
            break;
          case "describe":
            if (!params.table) return { content: [{ type: "text", text: "❌ `table` required." }] };
            result = await actionDescribe(params.table);
            break;
          case "check":
            result = await actionCheck(params);
            break;
          case "aggregate":
            result = await actionAggregate(params);
            break;
          case "detail":
            result = await actionDetail(params);
            break;
          case "correlate":
            result = await actionCorrelate(params);
            break;
          default:
            return {
              content: [
                {
                  type: "text",
                  text: `❌ Unknown action "${params.action}". Valid: discover, check, aggregate, detail, correlate, describe`,
                },
              ],
            };
        }

        if (params.exclude_normal) {
          params.exclude_normal.forEach((n) => normalSet.add(n));
        }
        return result;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: "text", text: `❌ Error: ${msg}` }], details: { error: msg } };
      }
    },

    setLevel(level) {
      if (LEVEL_CONFIG[level]) currentLevel = level;
    },
    reset() {
      currentLevel = "L0";
      queriedRanges.clear();
      normalSet.clear();
    },
  };
}
