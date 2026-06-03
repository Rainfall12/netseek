import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createQueryPgSqlTool } from "./src/tool.js";

export default definePluginEntry({
  id: "pgsql-query",
  name: "PostgreSQL Query",
  description:
    "Query PostgreSQL tables for network device data (inventory, config, topology, metrics)",
  register(api) {
    api.registerTool(createQueryPgSqlTool());
  },
});
