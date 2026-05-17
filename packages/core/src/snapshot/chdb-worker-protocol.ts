import * as Schema from "effect/Schema";
import type { RuntimeAggregateMap, RuntimeComparator, SortDirection } from "../protocol/index.ts";
import type { WorkerVersion } from "../worker/mutation-log.ts";

export const CHDB_WORKER_PROTOCOL_VERSION = 1;

export const CHDB_WORKER_REQUEST_TYPES = [
  "init",
  "initStart",
  "initRows",
  "initCommit",
  "applyBatch",
  "snapshot",
  "groupedRefreshSnapshot",
  "health",
  "close",
] as const;

export type ChdbWorkerRequestType = (typeof CHDB_WORKER_REQUEST_TYPES)[number];

export type ChdbWireBigDecimal = {
  readonly _tag: "ViewServerBigDecimal";
  readonly value: string;
};

export type ChdbWireValue =
  | ChdbWireBigDecimal
  | string
  | number
  | bigint
  | boolean
  | null
  | undefined
  | readonly ChdbWireValue[]
  | { readonly [key: string]: ChdbWireValue };

export type ChdbWireRow = Readonly<Record<string, ChdbWireValue>>;

export type ChdbWireVersionedRow = {
  readonly row: ChdbWireRow;
  readonly version: WorkerVersion;
};

export type ChdbWireMutationLogEntry = {
  readonly version: WorkerVersion;
  readonly kind: "insert" | "update" | "delete";
  readonly id: string | number;
  readonly before?: ChdbWireRow | undefined;
  readonly after?: ChdbWireRow | undefined;
  readonly changedFields: ReadonlySet<string>;
};

export type ChdbWireFilterNode =
  | {
      readonly field: string;
      readonly comparator: RuntimeComparator;
      readonly value: ChdbWireValue;
    }
  | {
      readonly op: "and" | "or";
      readonly conditions: readonly ChdbWireFilterNode[];
    };

export type ChdbWireOrderBy = readonly {
  readonly field: string;
  readonly direction: SortDirection;
}[];

export type ChdbWireRuntimeRawQuery = {
  readonly fields: Readonly<Record<string, true>>;
  readonly where?: ChdbWireFilterNode | undefined;
  readonly orderBy?: ChdbWireOrderBy | undefined;
  readonly offset?: number | undefined;
  readonly limit?: number | undefined;
};

export type ChdbWireRuntimeGroupedQuery = {
  readonly groupBy: readonly string[];
  readonly aggregates: RuntimeAggregateMap;
  readonly where?: ChdbWireFilterNode | undefined;
  readonly orderBy?: ChdbWireOrderBy | undefined;
  readonly offset?: number | undefined;
  readonly limit?: number | undefined;
};

export type ChdbWireRuntimeQuery = ChdbWireRuntimeRawQuery | ChdbWireRuntimeGroupedQuery;

export type ChdbWireSnapshotBackendResult = {
  readonly rows: readonly ChdbWireRow[];
  readonly totalRows: number;
  readonly backendVersion: WorkerVersion;
  readonly replayRows?: readonly ChdbWireRow[] | undefined;
};

export type ChdbWireHealth = {
  readonly status: "ready" | "degraded" | "restarting" | "stopped";
  readonly message?: string | undefined;
  readonly pid?: number | undefined;
  readonly restarts?: number | undefined;
  readonly pendingRequests?: number | undefined;
  readonly lastError?: string | undefined;
  readonly backendVersion?: WorkerVersion | undefined;
};

export type ChdbWorkerInitRequest = {
  readonly id: number;
  readonly type: "init";
  readonly args: {
    readonly topic: string;
    readonly idField: string;
    readonly rows: readonly ChdbWireVersionedRow[];
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  };
};

export type ChdbWorkerInitStartRequest = {
  readonly id: number;
  readonly type: "initStart";
  readonly args: {
    readonly topic: string;
    readonly idField: string;
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  };
};

export type ChdbWorkerInitRowsRequest = {
  readonly id: number;
  readonly type: "initRows";
  readonly rows: readonly ChdbWireVersionedRow[];
};

export type ChdbWorkerInitCommitRequest = {
  readonly id: number;
  readonly type: "initCommit";
};

export type ChdbWorkerApplyBatchRequest = {
  readonly id: number;
  readonly type: "applyBatch";
  readonly args: {
    readonly mutations: readonly ChdbWireMutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  };
};

export type ChdbWorkerSnapshotRequest = {
  readonly id: number;
  readonly type: "snapshot";
  readonly args: {
    readonly query: ChdbWireRuntimeQuery;
    readonly targetVersion: WorkerVersion;
  };
};

export type ChdbWorkerGroupedRefreshSnapshotRequest = {
  readonly id: number;
  readonly type: "groupedRefreshSnapshot";
  readonly args: {
    readonly query: ChdbWireRuntimeQuery;
    readonly targetVersion: WorkerVersion;
  };
};

export type ChdbWorkerHealthRequest = {
  readonly id: number;
  readonly type: "health";
};

export type ChdbWorkerCloseRequest = {
  readonly id: number;
  readonly type: "close";
};

export type ChdbWorkerRequest =
  | ChdbWorkerInitRequest
  | ChdbWorkerInitStartRequest
  | ChdbWorkerInitRowsRequest
  | ChdbWorkerInitCommitRequest
  | ChdbWorkerApplyBatchRequest
  | ChdbWorkerSnapshotRequest
  | ChdbWorkerGroupedRefreshSnapshotRequest
  | ChdbWorkerHealthRequest
  | ChdbWorkerCloseRequest;

export type ChdbWorkerResponse =
  | {
      readonly id: number;
      readonly success: true;
      readonly result?: ChdbWireSnapshotBackendResult | undefined;
      readonly health?: ChdbWireHealth | undefined;
    }
  | {
      readonly id: number;
      readonly success: false;
      readonly error: string;
    };

export type ChdbWorkerSuccessResponse = Extract<ChdbWorkerResponse, { readonly success: true }>;

export const ChdbWireBigDecimal = Schema.Struct({
  _tag: Schema.Literal("ViewServerBigDecimal"),
  value: Schema.String,
});

export const ChdbWireValue: Schema.Codec<ChdbWireValue> = Schema.Union([
  ChdbWireBigDecimal,
  Schema.String,
  Schema.Number,
  Schema.BigInt,
  Schema.Boolean,
  Schema.Null,
  Schema.Undefined,
  Schema.Array(Schema.suspend((): Schema.Codec<ChdbWireValue> => ChdbWireValue)),
  Schema.Record(
    Schema.String,
    Schema.suspend((): Schema.Codec<ChdbWireValue> => ChdbWireValue),
  ),
]);

export const ChdbWireRow = Schema.Record(Schema.String, ChdbWireValue);

export const ChdbWireVersionedRow = Schema.Struct({
  row: ChdbWireRow,
  version: Schema.BigInt,
});

export const ChdbWireMutationLogEntry = Schema.Struct({
  version: Schema.BigInt,
  kind: Schema.Literals(["insert", "update", "delete"]),
  id: Schema.Union([Schema.String, Schema.Number]),
  before: Schema.optional(ChdbWireRow),
  after: Schema.optional(ChdbWireRow),
  changedFields: Schema.ReadonlySet(Schema.String),
});

const ChdbWorkerComparator = Schema.Literals([
  "equals",
  "not_equals",
  "greater_than",
  "greater_than_or_equal",
  "less_than",
  "less_than_or_equal",
  "contains",
  "starts_with",
  "one_of",
]);

export const ChdbWireFilterNode: Schema.Codec<ChdbWireFilterNode> = Schema.Union([
  Schema.Struct({
    field: Schema.String,
    comparator: ChdbWorkerComparator,
    value: ChdbWireValue,
  }),
  Schema.Struct({
    op: Schema.Literals(["and", "or"]),
    conditions: Schema.Array(
      Schema.suspend((): Schema.Codec<ChdbWireFilterNode> => ChdbWireFilterNode),
    ),
  }),
]);

export const ChdbWireOrderBy = Schema.Array(
  Schema.Struct({
    field: Schema.String,
    direction: Schema.Literals(["asc", "desc"]),
  }),
);

const ChdbWireAggregate = Schema.Union([
  Schema.Struct({
    aggFunc: Schema.Literals(["count", "count_distinct", "sum", "avg", "min", "max"]),
    field: Schema.String,
  }),
  Schema.Struct({
    aggFunc: Schema.Literals(["string_concat", "string_concat_distinct"]),
    field: Schema.String,
    joiner: Schema.String,
    sort: Schema.optional(Schema.Literals(["asc", "desc"])),
  }),
]);

export const ChdbWireRuntimeRawQuery = Schema.Struct({
  fields: Schema.Record(Schema.String, Schema.Literal(true)),
  where: Schema.optional(ChdbWireFilterNode),
  orderBy: Schema.optional(ChdbWireOrderBy),
  offset: Schema.optional(Schema.Number),
  limit: Schema.optional(Schema.Number),
});

export const ChdbWireRuntimeGroupedQuery = Schema.Struct({
  groupBy: Schema.Array(Schema.String),
  aggregates: Schema.Record(Schema.String, ChdbWireAggregate),
  where: Schema.optional(ChdbWireFilterNode),
  orderBy: Schema.optional(ChdbWireOrderBy),
  offset: Schema.optional(Schema.Number),
  limit: Schema.optional(Schema.Number),
});

export const ChdbWireRuntimeQuery = Schema.Union([
  ChdbWireRuntimeRawQuery,
  ChdbWireRuntimeGroupedQuery,
]);

export const ChdbWireSnapshotBackendResult = Schema.Struct({
  rows: Schema.Array(ChdbWireRow),
  totalRows: Schema.Number,
  backendVersion: Schema.BigInt,
  replayRows: Schema.optional(Schema.Array(ChdbWireRow)),
});

export const ChdbWireHealth = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "restarting", "stopped"]),
  message: Schema.optional(Schema.String),
  pid: Schema.optional(Schema.Number),
  restarts: Schema.optional(Schema.Number),
  pendingRequests: Schema.optional(Schema.Number),
  lastError: Schema.optional(Schema.String),
  backendVersion: Schema.optional(Schema.BigInt),
});

export const ChdbWorkerInitRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("init"),
  args: Schema.Struct({
    topic: Schema.String,
    idField: Schema.String,
    rows: Schema.Array(ChdbWireVersionedRow),
    version: Schema.BigInt,
    literalStringFields: Schema.optional(Schema.ReadonlySet(Schema.String)),
  }),
});

export const ChdbWorkerInitStartRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("initStart"),
  args: Schema.Struct({
    topic: Schema.String,
    idField: Schema.String,
    version: Schema.BigInt,
    literalStringFields: Schema.optional(Schema.ReadonlySet(Schema.String)),
  }),
});

export const ChdbWorkerInitRowsRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("initRows"),
  rows: Schema.Array(ChdbWireVersionedRow),
});

export const ChdbWorkerInitCommitRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("initCommit"),
});

export const ChdbWorkerApplyBatchRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("applyBatch"),
  args: Schema.Struct({
    mutations: Schema.Array(ChdbWireMutationLogEntry),
    highestVersion: Schema.BigInt,
  }),
});

export const ChdbWorkerSnapshotRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("snapshot"),
  args: Schema.Struct({
    query: ChdbWireRuntimeQuery,
    targetVersion: Schema.BigInt,
  }),
});

export const ChdbWorkerGroupedRefreshSnapshotRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("groupedRefreshSnapshot"),
  args: Schema.Struct({
    query: ChdbWireRuntimeQuery,
    targetVersion: Schema.BigInt,
  }),
});

export const ChdbWorkerHealthRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("health"),
});

export const ChdbWorkerCloseRequest = Schema.Struct({
  id: Schema.Number,
  type: Schema.Literal("close"),
});

export const ChdbWorkerRequest = Schema.Union([
  ChdbWorkerInitRequest,
  ChdbWorkerInitStartRequest,
  ChdbWorkerInitRowsRequest,
  ChdbWorkerInitCommitRequest,
  ChdbWorkerApplyBatchRequest,
  ChdbWorkerSnapshotRequest,
  ChdbWorkerGroupedRefreshSnapshotRequest,
  ChdbWorkerHealthRequest,
  ChdbWorkerCloseRequest,
]);

export const ChdbWorkerSuccessResponse = Schema.Struct({
  id: Schema.Number,
  success: Schema.Literal(true),
  result: Schema.optional(ChdbWireSnapshotBackendResult),
  health: Schema.optional(ChdbWireHealth),
});

export const ChdbWorkerErrorResponse = Schema.Struct({
  id: Schema.Number,
  success: Schema.Literal(false),
  error: Schema.String,
});

export const ChdbWorkerResponse = Schema.Union([
  ChdbWorkerSuccessResponse,
  ChdbWorkerErrorResponse,
]);

export function decodeChdbWorkerRequest(value: unknown): ChdbWorkerRequest {
  return Schema.decodeUnknownSync(ChdbWorkerRequest)(value);
}

export function isChdbWorkerResponse(value: unknown): value is ChdbWorkerResponse {
  try {
    Schema.decodeUnknownSync(ChdbWorkerResponse)(value);
    return true;
  } catch {
    return false;
  }
}

export function chdbWorkerFailure(id: number, error: string): ChdbWorkerResponse {
  return {
    id,
    success: false,
    error,
  };
}

export function chdbWorkerSuccess(id: number): ChdbWorkerResponse {
  return {
    id,
    success: true,
  };
}

export function chdbWorkerSnapshotSuccess(
  id: number,
  result: ChdbWireSnapshotBackendResult,
): ChdbWorkerResponse {
  return {
    id,
    success: true,
    result,
  };
}

export function chdbWorkerHealthSuccess(id: number, health: ChdbWireHealth): ChdbWorkerResponse {
  return {
    id,
    success: true,
    health,
  };
}

export function chdbWorkerRequestId(value: unknown): number {
  if (isReadonlyRecord(value) && typeof value.id === "number") {
    return value.id;
  }
  return -1;
}

function isReadonlyRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null;
}

export type ChdbQueryWorkerInitRequest = ChdbWorkerInitRequest;
export type ChdbQueryWorkerInitStartRequest = ChdbWorkerInitStartRequest;
export type ChdbQueryWorkerInitRowsRequest = ChdbWorkerInitRowsRequest;
export type ChdbQueryWorkerInitCommitRequest = ChdbWorkerInitCommitRequest;
export type ChdbQueryWorkerApplyBatchRequest = ChdbWorkerApplyBatchRequest;
export type ChdbQueryWorkerSnapshotRequest = ChdbWorkerSnapshotRequest;
export type ChdbQueryWorkerGroupedRefreshSnapshotRequest = ChdbWorkerGroupedRefreshSnapshotRequest;
export type ChdbQueryWorkerHealthRequest = ChdbWorkerHealthRequest;
export type ChdbQueryWorkerCloseRequest = ChdbWorkerCloseRequest;
export type ChdbQueryWorkerRequest = ChdbWorkerRequest;
export type ChdbQueryWorkerResponse = ChdbWorkerResponse;
export type ChdbQueryWorkerSuccessResponse = ChdbWorkerSuccessResponse;
