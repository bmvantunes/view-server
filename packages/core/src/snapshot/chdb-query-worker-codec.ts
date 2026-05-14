import { BigDecimal } from "effect";
import {
  isRuntimeGroupedQuery,
  type RuntimeFilterNode,
  type RuntimeQuery,
  type RuntimeRow,
} from "../protocol/index.ts";
import type { MutationLogEntry } from "../worker/mutation-log.ts";
import type { SnapshotBackendResult, VersionedRow } from "./snapshot-backend.ts";
import type {
  ChdbWireBigDecimal,
  ChdbWireFilterNode,
  ChdbWireMutationLogEntry,
  ChdbWireRuntimeQuery,
  ChdbWireRow,
  ChdbWireSnapshotBackendResult,
  ChdbWireValue,
  ChdbWireVersionedRow,
} from "./chdb-query-worker-protocol.ts";

export function encodeVersionedRow(row: VersionedRow): ChdbWireVersionedRow {
  return {
    row: encodeRuntimeRow(row.row),
    version: row.version,
  };
}

export function decodeVersionedRow(row: ChdbWireVersionedRow): VersionedRow {
  return {
    row: decodeRuntimeRow(row.row),
    version: row.version,
  };
}

export function encodeMutationLogEntry(entry: MutationLogEntry): ChdbWireMutationLogEntry {
  return {
    version: entry.version,
    kind: entry.kind,
    id: entry.id,
    ...(entry.before === undefined ? {} : { before: encodeRuntimeRow(entry.before) }),
    ...(entry.after === undefined ? {} : { after: encodeRuntimeRow(entry.after) }),
    changedFields: entry.changedFields,
  };
}

export function decodeMutationLogEntry(entry: ChdbWireMutationLogEntry): MutationLogEntry {
  return {
    version: entry.version,
    kind: entry.kind,
    id: entry.id,
    ...(entry.before === undefined ? {} : { before: decodeRuntimeRow(entry.before) }),
    ...(entry.after === undefined ? {} : { after: decodeRuntimeRow(entry.after) }),
    changedFields: entry.changedFields,
  };
}

export function encodeRuntimeQuery(query: RuntimeQuery): ChdbWireRuntimeQuery {
  if (isRuntimeGroupedQuery(query)) {
    return {
      groupBy: query.groupBy,
      aggregates: query.aggregates,
      ...(query.where === undefined ? {} : { where: encodeFilterNode(query.where) }),
      ...(query.orderBy === undefined ? {} : { orderBy: query.orderBy }),
      ...(query.offset === undefined ? {} : { offset: query.offset }),
      ...(query.limit === undefined ? {} : { limit: query.limit }),
    };
  }
  return {
    fields: query.fields,
    ...(query.where === undefined ? {} : { where: encodeFilterNode(query.where) }),
    ...(query.orderBy === undefined ? {} : { orderBy: query.orderBy }),
    ...(query.offset === undefined ? {} : { offset: query.offset }),
    ...(query.limit === undefined ? {} : { limit: query.limit }),
  };
}

export function decodeRuntimeQuery(query: ChdbWireRuntimeQuery): RuntimeQuery {
  if (isWireGroupedQuery(query)) {
    return {
      groupBy: query.groupBy,
      aggregates: query.aggregates,
      ...(query.where === undefined ? {} : { where: decodeFilterNode(query.where) }),
      ...(query.orderBy === undefined ? {} : { orderBy: query.orderBy }),
      ...(query.offset === undefined ? {} : { offset: query.offset }),
      ...(query.limit === undefined ? {} : { limit: query.limit }),
    };
  }
  return {
    fields: query.fields,
    ...(query.where === undefined ? {} : { where: decodeFilterNode(query.where) }),
    ...(query.orderBy === undefined ? {} : { orderBy: query.orderBy }),
    ...(query.offset === undefined ? {} : { offset: query.offset }),
    ...(query.limit === undefined ? {} : { limit: query.limit }),
  };
}

function isWireGroupedQuery(
  query: ChdbWireRuntimeQuery,
): query is Extract<ChdbWireRuntimeQuery, { readonly groupBy: readonly string[] }> {
  return "groupBy" in query && Array.isArray(query.groupBy);
}

export function encodeSnapshotBackendResult(
  result: SnapshotBackendResult,
): ChdbWireSnapshotBackendResult {
  return {
    rows: result.rows.map(encodeRuntimeRow),
    totalRows: result.totalRows,
    backendVersion: result.backendVersion,
    ...(result.replayRows === undefined
      ? {}
      : { replayRows: result.replayRows.map(encodeRuntimeRow) }),
  };
}

export function decodeSnapshotBackendResult(
  result: ChdbWireSnapshotBackendResult,
): SnapshotBackendResult {
  return {
    rows: result.rows.map(decodeRuntimeRow),
    totalRows: result.totalRows,
    backendVersion: result.backendVersion,
    ...(result.replayRows === undefined
      ? {}
      : { replayRows: result.replayRows.map(decodeRuntimeRow) }),
  };
}

function encodeRuntimeRow(row: RuntimeRow): ChdbWireRow {
  return Object.fromEntries<ChdbWireValue>(
    Object.entries(row).map(([key, value]) => [key, encodeWireValue(value)]),
  );
}

function decodeRuntimeRow(row: ChdbWireRow): RuntimeRow {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, decodeWireValue(value)]),
  );
}

function encodeFilterNode(filter: RuntimeFilterNode): ChdbWireFilterNode {
  if ("op" in filter) {
    return {
      op: filter.op,
      conditions: filter.conditions.map(encodeFilterNode),
    };
  }
  return {
    field: filter.field,
    comparator: filter.comparator,
    value: encodeWireValue(filter.value),
  };
}

function decodeFilterNode(filter: ChdbWireFilterNode): RuntimeFilterNode {
  if ("op" in filter) {
    return {
      op: filter.op,
      conditions: filter.conditions.map(decodeFilterNode),
    };
  }
  return {
    field: filter.field,
    comparator: filter.comparator,
    value: decodeWireValue(filter.value),
  };
}

function encodeWireValue(value: unknown): ChdbWireValue {
  if (BigDecimal.isBigDecimal(value)) {
    return {
      _tag: "ViewServerBigDecimal",
      value: BigDecimal.format(value),
    };
  }
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(encodeWireValue);
  }
  if (typeof value === "object") {
    return Object.fromEntries<ChdbWireValue>(
      Object.entries(value).map(([key, entry]) => [key, encodeWireValue(entry)]),
    );
  }
  return undefined;
}

function decodeWireValue(value: ChdbWireValue): unknown {
  if (isWireBigDecimal(value)) {
    return BigDecimal.fromStringUnsafe(value.value);
  }
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(decodeWireValue);
  }
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [key, decodeWireValue(entry)]),
  );
}

function isWireBigDecimal(value: ChdbWireValue): value is ChdbWireBigDecimal {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "_tag" in value &&
    value._tag === "ViewServerBigDecimal"
  );
}
