import * as Schema from "effect/Schema";
import type { TopicConfig, TopicConfigMap, ViewServerConfig } from "./define-config.ts";

export function readViewServerConfigExport(
  moduleValue: unknown,
  exportName?: string,
): ViewServerConfig {
  if (!isRecord(moduleValue)) {
    throw new Error("Config module did not resolve to an object");
  }
  const config =
    exportName === undefined
      ? (moduleValue.default ?? moduleValue.config ?? moduleValue.viewServerConfig)
      : moduleValue[exportName];
  if (!isViewServerConfig(config)) {
    const suffix = exportName === undefined ? "" : ` named ${exportName}`;
    throw new Error(`Config module must export a defineConfig result${suffix}`);
  }
  return config;
}

export function isViewServerConfig(value: unknown): value is ViewServerConfig {
  return isRecord(value) && isTopicConfigMap(value.topics);
}

function isTopicConfigMap(value: unknown): value is TopicConfigMap {
  if (!isRecord(value)) {
    return false;
  }
  return Object.values(value).every(isTopicConfig);
}

function isTopicConfig(value: unknown): value is TopicConfig {
  return isRecord(value) && typeof value.id === "string" && Schema.isSchema(value.schema);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
