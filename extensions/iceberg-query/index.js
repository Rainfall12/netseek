import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createQueryIcebergTool } from "./src/tool.js";

export default definePluginEntry({
  id: "iceberg-query",
  name: "Iceberg Query",
  description: "Query local Iceberg tables for network device data",
  register(api) {
    api.registerTool(createQueryIcebergTool());
  },
});
