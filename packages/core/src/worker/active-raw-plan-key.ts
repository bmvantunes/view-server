import * as BigDecimal from "effect/BigDecimal";
import type { RuntimeRawQuery } from "../protocol/index.ts";
import { rawQueryOrderBy } from "./query-engine.ts";

export const ACTIVE_RAW_PLAN_KEY_CACHE_SCOPE = "topic" as const;

export function activeRawPlanKey(query: RuntimeRawQuery, idField: string): string {
  // The key intentionally omits projection/window fields and schema-derived execution options
  // such as literalStringFields. ActiveRawPlan caches are valid only inside one topic worker,
  // because each topic has exactly one schema/options set and no cross-topic subscriptions exist.
  return stableStringify({
    orderBy: rawQueryOrderBy(query, idField),
    where: query.where ?? null,
  });
}

export function stableStringify(value: unknown): string {
  if (typeof value === "bigint") {
    return `{"$bigint":${JSON.stringify(value.toString())}}`;
  }
  if (BigDecimal.isBigDecimal(value)) {
    return `{"$bigdecimal":${JSON.stringify(BigDecimal.format(value))}}`;
  }
  if (value === undefined) {
    return '{"$undefined":true}';
  }
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
  return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`).join(",")}}`;
}
