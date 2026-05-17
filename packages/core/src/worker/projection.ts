import {
  rowKeyByField,
  type RuntimeRawQuery,
  type RuntimeRow,
  type RuntimeRowKey,
} from "../protocol/index.ts";
import { materializeQueryValue, valuesEqual } from "../protocol/query-semantics.ts";
import { stableKeyEquals } from "../protocol/stable-key.ts";
import type { MutationLogEntry } from "./mutation-log.ts";

export function projectRow(
  row: RuntimeRow,
  fields: RuntimeRawQuery["fields"],
  idField: string,
): RuntimeRow {
  const projected: RuntimeRow = {};
  for (const [field, enabled] of Object.entries(fields)) {
    if (enabled) {
      projected[field] = materializeQueryValue(row[field]);
    }
  }
  projected[idField] = row[idField];
  return projected;
}

export function projectedRowsEqual(left: RuntimeRow | undefined, right: RuntimeRow): boolean {
  if (left === undefined) {
    return false;
  }
  const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
  for (const key of keys) {
    if (!valuesEqual(materializeQueryValue(left[key]), materializeQueryValue(right[key]), true)) {
      return false;
    }
  }
  return true;
}

export function projectedFieldsMayHaveChanged(args: {
  readonly fields: RuntimeRawQuery["fields"];
  readonly idField: string;
  readonly changedFields: ReadonlySet<string>;
}): boolean {
  if (args.changedFields.has(args.idField)) {
    return true;
  }
  for (const [field, enabled] of Object.entries(args.fields)) {
    if (enabled && args.changedFields.has(field)) {
      return true;
    }
  }
  return false;
}

export function visibleNonProjectedUpdateNoop(args: {
  readonly mutation: MutationLogEntry;
  readonly fields: RuntimeRawQuery["fields"];
  readonly idField: string;
  readonly visibleIds: readonly RuntimeRowKey[];
}): boolean {
  const mutation = args.mutation;
  if (mutation.kind !== "update" || mutation.before === undefined || mutation.after === undefined) {
    return false;
  }
  if (
    projectedFieldsMayHaveChanged({
      fields: args.fields,
      idField: args.idField,
      changedFields: mutation.changedFields,
    })
  ) {
    return false;
  }
  const beforeId = rowKeyByField(mutation.before, args.idField);
  const afterId = rowKeyByField(mutation.after, args.idField);
  return args.visibleIds.some(
    (id) =>
      stableKeyEquals(id, beforeId) ||
      stableKeyEquals(id, afterId) ||
      stableKeyEquals(id, mutation.id),
  );
}
