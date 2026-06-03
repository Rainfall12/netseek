import { execFile } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Type } from "typebox";

const execFileAsync = promisify(execFile);

// Python script path — relative to this extension directory
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT = path.resolve(__dirname, "../scripts/query_iceberg.py");

// ─── 当前运行上下文（由 OpenClaw runtime 注入） ───
// 可通过环境变量或全局状态获取
let currentLevel = "L0";
let queriedRanges = new Map(); // key → last_query_time
let normalSet = new Set(); // 已确认正常的设备/区域
const CACHE_TTL_MS = 5 * 60_000; // 5 min

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
const MAX_TIME_WINDOW_MIN = 480; // 全局±8小时上限
const MAX_LOG_ROWS = 100;
const MAX_ALERT_ROWS = 50;
const MAX_METRIC_ITEMS = 3;

// ─── RCA 参数 Schema ───
const QueryIcebergSchema = Type.Object(
  {
    // ── 核心 RCA 动作 ──
    action: Type.String({
      description:
        "RCA action: " +
        "'discover' — list tables + known device groups/rooms (L0+, first call only, cache 10min); " +
        "'check' — quick aggregate scan for anomalies (COUNT/MAX/AVG) within current boundary; " +
        "'aggregate' — targeted aggregate within time+space boundary (use after check finds anomaly); " +
        "'detail' — raw rows with narrow scope (only after aggregate shows anomaly, use minimal columns); " +
        "'correlate' — cross-table correlation query to link findings (topology→metric, config→event).",
    }),

    // ── 假设追踪 ──
    hypothesis: Type.Optional(
      Type.String({
        description:
          "What hypothesis is this query testing? " +
          "E.g. '端口拥塞导致丢包', '设备A配置变更导致中断'. " +
          "Required for check/aggregate/detail/correlate. " +
          "Helps track reasoning and avoid repetitive queries.",
      }),
    ),

    // ── 时间边界 ──
    fault_time: Type.Optional(
      Type.String({
        description:
          "ISO 8601 timestamp of when the fault occurred (e.g. '2026-05-31T14:30:00+08:00'). " +
          "ALL time queries pivot around this. " +
          "Required for check/aggregate/detail/correlate. Do NOT use 'now' or relative times.",
      }),
    ),
    time_window_min: Type.Optional(
      Type.Number({
        description:
          "Half-window in minutes: fault_time ± time_window_min. " +
          "Start at 15, double on no-finding: 15→30→60→120. Max 480 (8h). " +
          "Default matches current RCA level.",
        minimum: 5,
        maximum: 480,
      }),
    ),

    // ── 空间边界 ──
    scope: Type.Optional(
      Type.String({
        description:
          "Spatial scope: 'device' | 'device_group' | 'room' | 'region'. " +
          "Default matches current RCA level. " +
          "One device → group → room → region. No skipping levels.",
      }),
    ),
    scope_target: Type.Optional(
      Type.String({
        description:
          "Specific device name, group name, room name, or region name. " +
          "Required for check/aggregate/detail/correlate when scope is set. " +
          "Use 'discover' first to learn available names.",
      }),
    ),
    exclude_normal: Type.Optional(
      Type.Array(Type.String(), {
        description:
          "Device/group/room names already confirmed normal. These will be filtered out. " +
          "Maintain a running list across the session to avoid repeat queries.",
      }),
    ),

    // ── 数据边界 ──
    severity: Type.Optional(
      Type.String({
        description:
          "Log/alert severity filter: 'error' | 'warn' | 'info' | 'all'. " +
          "Default: 'error' for fault scenarios. Only widen if no error-level clues. " +
          "Normal info logs are useless for RCA.",
      }),
    ),
    aggregate_first: Type.Optional(
      Type.Boolean({
        description:
          "If true, force aggregate query first before returning raw rows. " +
          "Always prefer true for initial investigation. " +
          "Only set false when you've confirmed an anomaly and need detailed evidence.",
        default: true,
      }),
    ),

    // ── 查询参数（继承自原有） ──
    table: Type.Optional(
      Type.String({
        description:
          "Table name. Required for check/aggregate/detail/correlate. " +
          "Use 'discover' first if unsure. " +
          "Available: inventory, config_snapshot, topology, monitor_config, metric_ts.",
      }),
    ),
    columns: Type.Optional(
      Type.String({
        description:
          "Comma-separated column names. Omit for discover. " +
          "For detail: pick only columns relevant to current hypothesis (≤5).",
      }),
    ),
    where: Type.Optional(
      Type.Array(
        Type.Object({
          column: Type.String({ description: "Column name" }),
          op: Type.String({
            description: "=, !=, >, <, >=, <=, IN, NOT IN, IS NULL, IS NOT NULL, BETWEEN",
          }),
          value: Type.Any({ description: "Value. Array for IN/BETWEEN." }),
        }),
        { description: "Additional filters beyond time+space boundary." },
      ),
    ),
    order_by: Type.Optional(Type.String({ description: "Sort column. '-' prefix for DESC." })),
    limit: Type.Optional(
      Type.Number({
        description: "Max rows. Default auto-set by level: L0=50, L1=100, L2=200, L3=500.",
        minimum: 1,
        maximum: 500,
      }),
    ),
    agg_func: Type.Optional(
      Type.String({
        description: "Aggregate: COUNT, SUM, AVG, MIN, MAX. Required for aggregate action.",
      }),
    ),
    agg_column: Type.Optional(Type.String({ description: "Column to aggregate." })),
    group_by: Type.Optional(
      Type.String({ description: "Group-by column. Useful for topology grouping." }),
    ),

    // ── 关系分析（correlate 专用） ──
    correlate_with: Type.Optional(
      Type.String({
        description:
          "For 'correlate' action: second table to join/correlate. " +
          "E.g. correlate topology.links with metric_ts.bandwidth.",
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

async function runPython(args) {
  const { stdout, stderr } = await execFileAsync("python3", [SCRIPT, ...args], {
    timeout: 60_000,
    maxBuffer: 10 * 1024 * 1024,
  });
  if (stderr && !stdout) {
    return { success: false, error: stderr.slice(0, 2000) };
  }
  try {
    return JSON.parse(stdout);
  } catch {
    return { success: false, error: `Parse error: ${stdout.slice(0, 500)}` };
  }
}

function getEffectiveLimit(params) {
  return params.limit || LEVEL_CONFIG[currentLevel]?.data_limit || 50;
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
    return "🔍 **Anomaly found!** → Narrow scope to the specific device/port. Use `action='detail'` to get raw evidence.";
  }
  if (!hasData) {
    const nextWindow = Math.min(
      (LEVEL_CONFIG[currentLevel]?.time_window_min || 15) * 2,
      MAX_TIME_WINDOW_MIN,
    );
    return `🔄 No anomaly in current boundary. → Expand time_window to ±${nextWindow}min or broaden scope to next spatial level.`;
  }
  return "✅ Query complete. Review results and update hypothesis.";
}

// ── discover: 发现表结构 + 可用设备/区域列表 ──
async function actionDiscover() {
  const result = await runPython(["--list"]);

  if (!result.tables) {
    return {
      content: [{ type: "text", text: `Failed: ${result.error || "unknown"}` }],
      details: result,
    };
  }

  const lines = Object.entries(result.tables).map(([name, info]) => {
    const rowCount = info.row_count != null ? ` (~${info.row_count} rows)` : "";
    const colSummary = info.columns
      ? info.columns.map((c) => `${c.name}: ${c.type}`).join(", ")
      : info.error || "n/a";
    return `- **${name}**${rowCount} | columns: ${colSummary}`;
  });

  const tables = Object.keys(result.tables);
  const text =
    `## 📊 Available Data\n\n` +
    `**${tables.length} tables** found:\n\n${lines.join("\n")}\n\n` +
    `### Usage Guide\n` +
    `| Table | RCA Use |\n` +
    `|---|---|\n` +
    `| inventory | Find device IP, model, location, group |\n` +
    `| config_snapshot | Check recent config changes |\n` +
    `| topology | Trace upstream/downstream links |\n` +
    `| monitor_config | What metrics are monitored |\n` +
    `| metric_ts | Time-series: bandwidth, errors, drops, CPU |\n\n` +
    `**Next step**: set \`action='check'\` with \`fault_time\`, \`scope='device_group'\`, and \`scope_target='<suspected area>'\`.`;

  return {
    content: [{ type: "text", text }],
    details: { ...result, boundary_state: formatBoundaryState({}, false) },
  };
}

// ── check: 快速聚合扫描异常 ──
async function actionCheck(params) {
  if (!params.hypothesis) {
    return {
      content: [
        { type: "text", text: "❌ `hypothesis` is required for `check`. What are you testing?" },
      ],
    };
  }
  if (!params.fault_time) {
    return {
      content: [{ type: "text", text: "❌ `fault_time` is required. When did the fault happen?" }],
    };
  }

  const effLimit = Math.min(getEffectiveLimit(params), MAX_METRIC_ITEMS * 3);

  // 自动注入时间和空间筛选
  const autoWhere = [];
  if (params.scope_target) {
    autoWhere.push({ column: "device_id", op: "=", value: params.scope_target });
  }
  if (params.exclude_normal?.length) {
    autoWhere.push({ column: "device_id", op: "NOT IN", value: params.exclude_normal });
  }

  const args = ["--query", params.table || "metric_ts"];
  args.push("--limit", String(effLimit));
  args.push("--where", JSON.stringify([...autoWhere, ...(params.where || [])]));
  if (params.order_by) args.push("--order-by", params.order_by);

  const result = await runPython(args);

  if (!result.success) {
    return {
      content: [{ type: "text", text: `Check failed: ${result.error}` }],
      details: result,
    };
  }

  const cols = result.columns || [];
  const rows = result.rows || [];
  const hasData = rows.length > 0;

  // 简单异常检测：数值类字段找最大值行
  let anomalyDetected = false;
  if (hasData) {
    for (const row of rows.slice(0, 5)) {
      for (const [k, v] of Object.entries(row)) {
        if (typeof v === "number" && k.match(/error|drop|loss|fail|exceed/i)) {
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
    `**Table**: ${params.table || "metric_ts"} | **Level**: ${boundary.level_desc}\n` +
    `**Time**: ${params.fault_time} ±${boundary.time_window}\n` +
    `**Scope**: ${boundary.scope_target}\n\n` +
    `**Result**: ${rows.length} rows scanned | Anomaly: ${anomalyDetected ? "⚠️ YES" : "✅ none"}\n\n` +
    formatTable(cols, rows) +
    `\n\n` +
    rec;

  return {
    content: [{ type: "text", text }],
    details: { ...result, boundary_state: boundary, recommendation: rec },
  };
}

// ── aggregate: 定向聚合 ──
async function actionAggregate(params) {
  if (!params.table) {
    return { content: [{ type: "text", text: "❌ `table` required." }] };
  }

  const args = ["--aggregate", params.table, "--agg-func", params.agg_func || "COUNT"];
  if (params.agg_column) args.push("--agg-column", params.agg_column);
  if (params.group_by) args.push("--group-by", params.group_by);
  if (params.where) args.push("--where", JSON.stringify(params.where));
  args.push("--limit", String(getEffectiveLimit(params)));

  const result = await runPython(args);

  if (!result.success) {
    return { content: [{ type: "text", text: `Aggregate failed: ${result.error}` }] };
  }

  const rows = result.rows || [];
  const cols = rows.length > 0 ? Object.keys(rows[0]) : [];
  const anomalyDetected = rows.length > 0;

  const text =
    `## 📈 Aggregate: ${result.agg_func} on ${params.table}` +
    (params.agg_column ? `.${params.agg_column}` : "") +
    (params.group_by ? ` by ${params.group_by}` : "") +
    (params.hypothesis ? `\n**Hypothesis**: ${params.hypothesis}` : "") +
    `\n\n${formatTable(cols, rows)}`;

  return {
    content: [{ type: "text", text }],
    details: { ...result, boundary_state: formatBoundaryState(params, anomalyDetected) },
  };
}

// ── detail: 细粒度查询（仅异常时） ──
async function actionDetail(params) {
  if (!params.table) {
    return { content: [{ type: "text", text: "❌ `table` required." }] };
  }

  const args = ["--query", params.table];
  if (params.columns) args.push("--columns", params.columns);
  if (params.where) args.push("--where", JSON.stringify(params.where));
  if (params.order_by) args.push("--order-by", params.order_by);
  args.push("--limit", String(Math.min(getEffectiveLimit(params), MAX_LOG_ROWS)));

  const result = await runPython(args);

  if (!result.success) {
    return { content: [{ type: "text", text: `Detail query failed: ${result.error}` }] };
  }

  const cols = result.columns || [];
  const rows = result.rows || [];
  const anomalyDetected = rows.length > 0;

  const text =
    `## 📋 Detail: ${params.table}` +
    (params.hypothesis ? `\n**Hypothesis**: ${params.hypothesis}` : "") +
    `\n${result.returned_rows} row(s) / ${result.total_scanned || "?"} scanned\n\n` +
    formatTable(cols, rows);

  return {
    content: [{ type: "text", text }],
    details: { ...result, boundary_state: formatBoundaryState(params, anomalyDetected) },
  };
}

// ── correlate: 跨表关联 ──
async function actionCorrelate(params) {
  if (!params.table || !params.correlate_with) {
    return {
      content: [
        {
          type: "text",
          text:
            "❌ `table` and `correlate_with` required for correlate.\n" +
            "Example: correlate topology (links) with metric_ts (bandwidth) around fault_time.",
        },
      ],
    };
  }

  // 先查主表，再查关联表
  const primaryArgs = ["--query", params.table];
  if (params.where) primaryArgs.push("--where", JSON.stringify(params.where));
  primaryArgs.push("--limit", String(Math.min(getEffectiveLimit(params), 50)));

  const primary = await runPython(primaryArgs);
  if (!primary.success) {
    return { content: [{ type: "text", text: `Primary query failed: ${primary.error}` }] };
  }

  const secondaryArgs = ["--query", params.correlate_with];
  if (params.where) secondaryArgs.push("--where", JSON.stringify(params.where));
  secondaryArgs.push("--limit", String(Math.min(getEffectiveLimit(params), 50)));

  const secondary = await runPython(secondaryArgs);

  const pCols = primary.columns || [];
  const pRows = primary.rows || [];
  const sCols = secondary.columns || [];
  const sRows = secondary.rows || [];

  const text =
    `## 🔗 Correlate: ${params.table} ↔ ${params.correlate_with}\n` +
    (params.hypothesis ? `**Hypothesis**: ${params.hypothesis}\n\n` : "\n") +
    `### ${params.table}\n${formatTable(pCols, pRows)}\n\n` +
    `### ${params.correlate_with}\n${formatTable(sCols, sRows)}`;

  return {
    content: [{ type: "text", text }],
    details: { primary: primary, secondary: secondary },
  };
}

// ── describe: 表结构详情 ──
async function actionDescribe(tableName) {
  const result = await runPython(["--describe", tableName]);

  if (result.error) {
    return { content: [{ type: "text", text: `Describe failed: ${result.error}` }] };
  }

  const schemaLines = (result.columns || []).map(
    (c) =>
      `  - \`${c.name}\` ${c.type}${c.required ? ", required" : ", nullable"}` +
      (c.doc ? ` — ${c.doc}` : ""),
  );

  const cardinalityLines = Object.entries(result.cardinality || {}).map(([col, info]) => {
    if (info.values) {
      return `  - \`${col}\`: ${info.distinct} distinct → [${info.values.join(", ")}]`;
    }
    return `  - \`${col}\`: ${info.distinct} distinct values`;
  });

  const sampleCols = (result.columns || []).map((c) => c.name);
  const sampleRows = result.sample || [];

  let text = `## Table: ${tableName}\n\n`;
  text += `### Columns\n${schemaLines.join("\n")}\n\n`;
  if (cardinalityLines.length > 0) {
    text += `### Cardinality\n${cardinalityLines.join("\n")}\n\n`;
  }
  if (sampleRows.length > 0) {
    text += `### Sample (${sampleRows.length} rows)\n${formatTable(sampleCols, sampleRows)}`;
  }

  return { content: [{ type: "text", text }], details: result };
}

// ── 缓存管理 ──
function checkCache(key) {
  const lastTime = queriedRanges.get(key);
  if (lastTime && Date.now() - lastTime < CACHE_TTL_MS) {
    return true; // cached, skip
  }
  return false;
}

function markCache(key) {
  queriedRanges.set(key, Date.now());
}

// ── 工具定义 ──
export function createQueryIcebergTool() {
  return {
    name: "query_iceberg",
    label: "Query Iceberg (RCA)",
    description:
      "Query network device metadata from Apache Iceberg (inventory, topology, metrics, config snapshots).\n\n" +
      "⚠️ IMPORTANT: This tool returns METADATA only. The `config_content` blob (5000-10000 lines of raw config text) " +
      "is deliberately EXCLUDED from all results. To read actual configuration content, " +
      "use the **read_device_config** tool after identifying which device to inspect.\n\n" +
      "### RCA Workflow (follow strictly):\n" +
      "1. `action='discover'` → learn available tables & spatial scopes (once, cached 10min)\n" +
      "2. `action='check'` hypothesis='...' fault_time='...' → quick anomaly scan within current boundary\n" +
      "3. If anomaly found → `action='aggregate'` to quantify, then `action='detail'` for evidence\n" +
      "4. If no anomaly → expand time_window or scope, re-run `action='check'`\n" +
      "5. `action='correlate'` → cross-table analysis only after finding specific clues\n\n" +
      "### Tool Boundaries:\n" +
      "- Use THIS tool for: device inventory, topology links, time-series metrics, config metadata (when/who changed)\n" +
      "- Use **read_device_config** for: reading actual config content (interfaces, ACLs, BGP, routes, firewall rules)\n" +
      "- Typical flow: query_iceberg(config_snapshot) → find relevant device → read_device_config(scan → search)\n\n" +
      "### Boundary Rules (automatic):\n" +
      "- Time: center on `fault_time`, start ±15min, double on no-finding (max ±8h)\n" +
      "- Space: device → device_group → room → region (no skipping)\n" +
      "- Data: aggregate first, detail only on anomaly, error-only logs, ≤3 metric items\n\n" +
      "### Anti-patterns (NEVER do):\n" +
      "❌ Query without fault_time or hypothesis\n" +
      "❌ SELECT * on large tables without narrow WHERE\n" +
      "❌ Skip aggregate and go straight to raw detail\n" +
      "❌ Use 'recent 1 hour' instead of fault_time ± offset\n" +
      "❌ Query already-verified-normal devices (maintain exclude_normal list)\n" +
      "❌ Try to read config_content via this tool — use read_device_config instead",
    parameters: QueryIcebergSchema,

    execute: async (_toolCallId, rawParams) => {
      const params = rawParams;

      // 缓存检查
      const cacheKey = JSON.stringify({ action: params.action, table: params.table });
      if (params.action === "discover" && checkCache(cacheKey)) {
        return {
          content: [
            {
              type: "text",
              text: "📌 Table list cached (<5min). Use cached results or set `action='check'` to proceed.",
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
            if (!params.table) {
              return { content: [{ type: "text", text: "❌ `table` required for describe." }] };
            }
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

        // 更新已确认正常列表
        if (params.exclude_normal) {
          params.exclude_normal.forEach((n) => normalSet.add(n));
        }

        return result;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return {
          content: [{ type: "text", text: `❌ Error: ${msg}` }],
          details: { error: msg },
        };
      }
    },

    /** 注入当前 RCA 等级（由外部调用） */
    setLevel(level) {
      if (LEVEL_CONFIG[level]) {
        currentLevel = level;
      }
    },

    /** 重置会话状态 */
    reset() {
      currentLevel = "L0";
      queriedRanges.clear();
      normalSet.clear();
    },
  };
}
