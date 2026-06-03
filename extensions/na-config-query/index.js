import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createReadNaConfigTool } from "./src/tool.js";

export default definePluginEntry({
  id: "na-config-query",
  name: "NA Config Query",
  description:
    "Read network device configuration blobs from NA/Cramer database. " +
    "Uses slice & fold strategy — no vendor-specific parsers, LLM interprets the content.",
  register(api) {
    api.registerTool(createReadNaConfigTool());
  },
});
