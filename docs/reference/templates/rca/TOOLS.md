# TOOLS.md - RCA 工具速查

## query_iceberg

### 调用模板

```
query_iceberg({
  table: "metric_ts" | "device_config" | "alert_log",
  hypothesis: "具体假设 — 查询目的",
  fault_time: "2024-06-01T14:30:00",
  level: "L0" | "L1" | "L2" | "L3",
  device_ids: ["DEV-SH-CORE-01"],
  metric_types: ["cpu", "memory", "interface_traffic"]  // 可选
})
```

### 表结构速查

**metric_ts** — 时序指标表
| 列 | 类型 | 说明 |
|----|------|------|
| device_id | string | 设备 ID |
| metric_type | string | 指标类型 (cpu/memory/interface_traffic/connection/latency/packet_loss) |
| metric_value | float | 指标值 |
| timestamp | timestamp | 时间戳 |
| tags | map | 附加标签 |

**device_config** — 设备配置表
| 列 | 类型 | 说明 |
|----|------|------|
| device_id | string | 设备 ID |
| config_type | string | running/startup |
| config_data | string | 配置内容 |
| last_updated | timestamp | 最后更新时间 |

**alert_log** — 告警日志表
| 列 | 类型 | 说明 |
|----|------|------|
| device_id | string | 设备 ID |
| alert_type | string | 告警类型 |
| severity | string | critical/high/medium/low |
| alert_time | timestamp | 告警时间 |

## read_device_config

### 调用模板

```
read_device_config({
  action: "scan" | "search" | "get_block",
  device_id: "DEV-SH-CORE-01",
  keyword: "bgp" | "ospf" | "interface"  // search 模式
  block_name: "interface GigabitEthernet" // get_block 模式
})
```

### 参数说明

- `scan`: 列出设备所有配置块名称
- `search`: 按关键词搜索配置内容
- `get_block`: 获取指定配置块完整内容

## 常用 pattern

```
# 流量异常排查
hypothesis: "出口设备 DEV-EX-01 带宽拥塞导致丢包"
→ metric_ts: metric_type=interface_traffic + packet_loss
→ device_config: search "bandwidth" + "qos"

# 设备宕机排查
hypothesis: "核心设备 DEV-CORE-01 CPU/内存异常导致服务中断"
→ metric_ts: metric_type=cpu + memory
→ alert_log: 查看同期告警

# BGP 路由异常
hypothesis: "BGP session down 导致流量中断"
→ device_config: search "bgp" → get_block BGP配置
→ metric_ts: metric_type=connection (BGP邻居状态)
```
