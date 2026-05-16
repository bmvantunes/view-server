import * as BigDecimal from "effect/BigDecimal";

export type StableKey = string | number;

export function isStableKey(value: unknown): value is StableKey {
  return typeof value === "string" || (typeof value === "number" && Number.isFinite(value));
}

export function stableKeyFromValue(value: unknown, label = "row id"): StableKey {
  if (isStableKey(value)) {
    return value;
  }
  throw new TypeError(
    `${label} must be a string or finite number, got ${stableKeyValueKind(value)}`,
  );
}

export function stableKeyFromRow(
  row: Readonly<Record<string, unknown>>,
  idField: string,
): StableKey {
  return stableKeyFromValue(row[idField], idField);
}

export function encodeStableKeyForWire(key: StableKey): StableKey {
  return stableKeyFromValue(key, "wire row key");
}

export function decodeStableKeyFromWire(value: unknown): StableKey {
  return stableKeyFromValue(value, "wire row key");
}

export function stableKeyEquals(left: StableKey, right: StableKey): boolean {
  return Object.is(left, right);
}

export function compareStableKeys(left: StableKey, right: StableKey): number {
  if (stableKeyEquals(left, right)) {
    return 0;
  }
  if (typeof left !== typeof right) {
    return typeof left === "number" ? -1 : 1;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }
  return String(left).localeCompare(String(right));
}

export function stableKeyDebug(key: StableKey): string {
  return `${typeof key}:${String(key)}`;
}

function stableKeyValueKind(value: unknown): string {
  if (BigDecimal.isBigDecimal(value)) {
    return "BigDecimal";
  }
  return value === null ? "null" : typeof value;
}
