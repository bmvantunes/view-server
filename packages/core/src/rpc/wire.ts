import * as BigDecimal from "effect/BigDecimal";
import { stableStringify, type RuntimeRow, type SubscriptionEvent } from "../protocol/index.ts";
import { encodeStableKeyForWire } from "../protocol/stable-key.ts";
import type { RpcWireValue } from "./rpcs.ts";

export function wireQueryResponse(response: {
  readonly rows: readonly RuntimeRow[];
  readonly totalRows: number;
  readonly version: string;
}) {
  return {
    rows: response.rows.map(toWireRow),
    totalRows: response.totalRows,
    version: response.version,
  };
}

export function wireSubscriptionEvent(event: SubscriptionEvent<readonly RuntimeRow[]>) {
  if (event.type === "snapshot") {
    return {
      ...event,
      rows: event.rows.map(toWireRow),
    };
  }
  if (event.type === "status") {
    return event;
  }
  return {
    ...event,
    ops: event.ops.map((operation) => {
      if (operation.type === "remove") {
        return {
          ...operation,
          key: encodeStableKeyForWire(operation.key),
        };
      }
      if (operation.type === "patch") {
        return {
          ...operation,
          key: encodeStableKeyForWire(operation.key),
          changes: toWireRow(operation.changes),
        };
      }
      return {
        ...operation,
        ...(operation.key === undefined ? {} : { key: encodeStableKeyForWire(operation.key) }),
        row: toWireRow(operation.row),
      };
    }),
  };
}

export function toWireRow(row: object): Readonly<Record<string, RpcWireValue>> {
  const wireRow: Record<string, RpcWireValue> = {};
  for (const [key, value] of Object.entries(row)) {
    const wireValue = toWireValue(value);
    if (wireValue !== undefined) {
      wireRow[key] = wireValue;
    }
  }
  return wireRow;
}

export function fromWireRow(row: Readonly<Record<string, RpcWireValue>>): RuntimeRow {
  return Object.fromEntries(Object.entries(row));
}

export function fromWireRows(
  rows: readonly Readonly<Record<string, RpcWireValue>>[],
): readonly RuntimeRow[] {
  return rows.map(fromWireRow);
}

function toWireValue(value: unknown): RpcWireValue | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  if (BigDecimal.isBigDecimal(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    const values: RpcWireValue[] = [];
    for (const entry of value) {
      const wireEntry = toWireValue(entry);
      if (wireEntry !== undefined) {
        values.push(wireEntry);
      }
    }
    return values;
  }
  if (typeof value === "object") {
    return toWireRow(value);
  }
  return stableStringify(value);
}
