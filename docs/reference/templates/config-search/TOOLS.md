# TOOLS.md — 工具速查

## query_iceberg

```json
// 按指标找设备
{"action":"check","table":"metric_ts","hypothesis":"CRC errors","fault_time":"2026-06-03T02:30:00+08:00",
 "where":[{"column":"metric_name","op":"=","value":"crc_error"},{"column":"value","op":">","value":100}]}

// 按设备属性找
{"action":"check","table":"inventory",
 "where":[{"column":"site","op":"=","value":"北京"}]}

// 了解表
{"action":"discover"}
```

**注意**: 所有表用 `device_id` 不是 `device_name`

## read_device_config

```json
// 扫描概览
{"device":"DEV-BJ-BR-04","mode":"scan"}

// 关键词搜索
{"device":"DEV-BJ-BR-04","mode":"search","keyword":"GigabitEthernet1/0/1"}

// 取指定块
{"device":"DEV-BJ-BR-04","mode":"get_block","block_index":3}
```

## 表结构速查

| 表              | 定位列                              | 用途       |
| --------------- | ----------------------------------- | ---------- |
| inventory       | device_id, site, vendor, model      | 找设备     |
| metric_ts       | device_id, metric_name, port, value | 按指标筛选 |
| config_snapshot | device_id, snapshot_time            | 找配置版本 |
| monitor_config  | device_id, metric_name              | 找监控配置 |
