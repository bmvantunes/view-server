import type { RuntimeAggregateMap, RuntimeComparator, SortDirection } from "../protocol/index.ts";
import type { WorkerVersion } from "../worker/mutation-log.ts";

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

export type ChdbQueryWorkerInitRequest = {
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

export type ChdbQueryWorkerInitStartRequest = {
  readonly id: number;
  readonly type: "initStart";
  readonly args: {
    readonly topic: string;
    readonly idField: string;
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  };
};

export type ChdbQueryWorkerInitRowsRequest = {
  readonly id: number;
  readonly type: "initRows";
  readonly rows: readonly ChdbWireVersionedRow[];
};

export type ChdbQueryWorkerInitCommitRequest = {
  readonly id: number;
  readonly type: "initCommit";
};

export type ChdbQueryWorkerApplyBatchRequest = {
  readonly id: number;
  readonly type: "applyBatch";
  readonly args: {
    readonly mutations: readonly ChdbWireMutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  };
};

export type ChdbQueryWorkerSnapshotRequest = {
  readonly id: number;
  readonly type: "snapshot";
  readonly args: {
    readonly query: ChdbWireRuntimeQuery;
    readonly targetVersion: WorkerVersion;
  };
};

export type ChdbQueryWorkerCloseRequest = {
  readonly id: number;
  readonly type: "close";
};

export type ChdbQueryWorkerRequest =
  | ChdbQueryWorkerInitRequest
  | ChdbQueryWorkerInitStartRequest
  | ChdbQueryWorkerInitRowsRequest
  | ChdbQueryWorkerInitCommitRequest
  | ChdbQueryWorkerApplyBatchRequest
  | ChdbQueryWorkerSnapshotRequest
  | ChdbQueryWorkerCloseRequest;

export type ChdbQueryWorkerResponse =
  | {
      readonly id: number;
      readonly success: true;
      readonly result?: ChdbWireSnapshotBackendResult | undefined;
    }
  | {
      readonly id: number;
      readonly success: false;
      readonly error: string;
    };

export type ChdbQueryWorkerSuccessResponse = Extract<
  ChdbQueryWorkerResponse,
  { readonly success: true }
>;
