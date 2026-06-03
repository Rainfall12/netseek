import { execFile } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Type } from "typebox";

const execFileAsync = promisify(execFile);
const __dirname = dirname(fileURLToPath(import.meta.url));

// ─── 工具参数 Schema ───
const ReadNaConfigSchema = Type.Object(
  {
    device: Type.String({
      description: "Device ID. E.g. 'SH-CORE-01', 'BJ-EDGE-RT-12'. Required.",
    }),

    snapshot_time: Type.Optional(
      Type.String({
        description:
          "ISO 8601 timestamp to retrieve a historical config snapshot. " +
          "If omitted, the latest config is returned.",
      }),
    ),

    mode: Type.String({
      description:
        "Read mode:\n" +
        "  'scan' — Get a folded overview of the entire config (~15 lines). " +
        "Similar blocks (e.g. 11 'interface GigabitEthernet' blocks) are collapsed into one line. " +
        "ALWAYS start with scan to understand the config structure.\n" +
        "  'search' — Search for blocks containing a keyword. " +
        "Returns full text of matched blocks (≤300 lines total). " +
        "Use after scan to dive into specific sections.\n" +
        "  'get_block' — Retrieve a single block by its index number " +
        "(as shown in the scan overview). " +
        "Use when you need one specific block's full text.",
    }),

    keyword: Type.Optional(
      Type.String({
        description:
          "Search keyword. Required when mode='search'. " +
          "Use terms from the scan overview like 'CCTV', 'VLAN', 'BGP', 'ACL'.",
      }),
    ),

    block_index: Type.Optional(
      Type.Number({
        description: "Block index number from scan overview. Required when mode='get_block'.",
        minimum: 1,
      }),
    ),
  },
  { additionalProperties: false },
);

// ─── 调用 slicer.py ───
async function runSlicer(args) {
  const slicerPath = join(__dirname, "slicer.py");
  const allArgs = [
    slicerPath,
    "--device",
    args.device,
    ...(args.snapshotTime ? ["--snapshot-time", args.snapshotTime] : []),
  ];

  if (args.mode === "scan") {
    allArgs.push("--scan");
  } else if (args.mode === "search") {
    allArgs.push("--search", args.keyword);
  } else if (args.mode === "get_block") {
    allArgs.push("--get-block", String(args.blockIndex));
  }

  // 继承环境变量（包含 ICEBERG_CATALOG_URI, S3_ENDPOINT 等）
  const { stdout } = await execFileAsync("python3", allArgs, {
    timeout: 30_000,
    maxBuffer: 2 * 1024 * 1024, // 2MB 够容纳 10000 行文本
  });

  return JSON.parse(stdout);
}

// ─── 格式化输出 ───
function formatOutput(result) {
  if (!result.success) {
    return `❌ 读取配置失败: ${result.error || "未知错误"}`;
  }

  const header = [
    `**设备**: ${result.device_id}`,
    `**更新时间**: ${result.updated_at}`,
    `**总块数**: ${result.total_chunks} | **总行数**: ${result.total_lines}`,
  ].join(" | ");

  if (result.overview) {
    // scan 模式：折叠概览
    return `## 📋 配置概览 — ${result.device_id}\n\n${header}\n\n\`\`\`\n${result.overview}\n\`\`\``;
  }

  if (result.text) {
    // search 模式：匹配块全文
    const count = result.match_count || 0;
    return `## 🔍 搜索 "${result.search}" — ${result.device_id}\n\n${header}\n\n找到 **${count}** 个匹配块\n\n\`\`\`\n${result.text}\n\`\`\``;
  }

  if (result.block) {
    // get_block 模式：单块
    return `## 📄 块 #${result.block_index} — ${result.device_id}\n\n${header}\n\n\`\`\`\n${result.block}\n\`\`\``;
  }

  return `✅ 读取成功: ${JSON.stringify(result, null, 2)}`;
}

// ─── 工具定义 ───
export function createReadNaConfigTool() {
  return {
    name: "read_device_config",
    label: "Read Device Config (Blob Slicer)",
    description:
      "Read the raw configuration content of a network device (the config blob). " +
      "Use this tool AFTER query_iceberg has identified which device/config to investigate.\n\n" +
      "⚠️ This is the ONLY tool that returns actual config text (interfaces, ACLs, BGP, routes, firewall rules). " +
      "The query_iceberg tool only returns metadata — use it first to find the right device, then switch here.\n\n" +
      "Configurations are large text blobs (5000–10000 lines per device). " +
      "This tool uses a **slice & fold** strategy to make them manageable:\n\n" +
      "### Workflow (follow strictly):\n" +
      "1. **`mode='scan'`** → Get a folded overview (~15 lines). " +
      "Identical blocks (e.g. 11 `interface GigabitEthernet`) are collapsed into one line with " +
      "the count and total lines. NO parsing — pure structural folding.\n" +
      "2. **`mode='search'` keyword='...'** → Find blocks matching a keyword. " +
      "Returns full text of matched blocks (≤300 lines). " +
      "Use terms visible in the scan overview.\n" +
      "3. **`mode='get_block'` block_index=N** → Retrieve a single block by its #index. " +
      "Use when scan shows a specific block you want to inspect.\n\n" +
      "### Historical Snapshots:\n" +
      "Use `snapshot_time='2026-06-03T02:30:00'` to query a past config version. " +
      "All config snapshots are retained in Iceberg (time-travel enabled).\n\n" +
      "### Tool Boundaries:\n" +
      "- query_iceberg = metadata (which device, when changed, what table)\n" +
      "- read_device_config = actual config text (interfaces, ACLs, BGP, firewall rules)\n" +
      "- Typical flow: query_iceberg(config_snapshot) → read_device_config(scan → search)\n\n" +
      "### Design Principles:\n" +
      "- **No vendor-specific parsers** — relies on universal delimiters (`!`, `}`, `#`, `</...>`)\n" +
      "- **LLM understands the config** — the tool only filters/slices, " +
      "you (the Agent) interpret the content\n" +
      "- **Two-step read**: scan → search/get_block. Never try to read the entire blob at once.\n\n" +
      "### Anti-patterns (NEVER do):\n" +
      "❌ Skip scan and search blindly\n" +
      "❌ Try to read all 5000+ lines at once\n" +
      "❌ Assume config structure — different vendors use different syntaxes\n" +
      "❌ Write vendor-specific regex — use keywords from the scan overview only\n" +
      "❌ Call this before query_iceberg has identified which device to inspect",

    parameters: ReadNaConfigSchema,

    execute: async (_toolCallId, rawParams) => {
      const params = rawParams;

      // 参数校验
      if (!params.device) {
        return {
          content: [{ type: "text", text: "❌ `device` required." }],
        };
      }

      if (params.mode === "search" && !params.keyword) {
        return {
          content: [
            {
              type: "text",
              text: "❌ `keyword` required when mode='search'.",
            },
          ],
        };
      }

      if (params.mode === "get_block" && !params.block_index) {
        return {
          content: [
            {
              type: "text",
              text: "❌ `block_index` required when mode='get_block'.",
            },
          ],
        };
      }

      try {
        const result = await runSlicer({
          device: params.device,
          mode: params.mode || "scan",
          keyword: params.keyword || null,
          blockIndex: params.block_index || null,
          snapshotTime: params.snapshot_time || null,
        });

        return {
          content: [{ type: "text", text: formatOutput(result) }],
          details: result,
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return {
          content: [{ type: "text", text: `❌ Error: ${msg}` }],
          details: { error: msg },
        };
      }
    },
  };
}
