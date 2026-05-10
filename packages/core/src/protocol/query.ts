import { BigDecimal } from "effect";
import type {
  ReadableTopicName,
  ReadableTopicRowFromConfig,
  TopicName,
  TopicRowFromConfig,
  ViewServerConfig,
} from "../config/index.ts";

export type SortDirection = "asc" | "desc";
export type SubscriptionStatus = "connecting" | "snapshot_loading" | "live" | "error" | "closed";

export interface TopicMap {
  readonly [topicName: string]: object;
}

export type FieldOf<TRow> = Extract<keyof TRow, string>;
export type NumericField<TRow> = Extract<
  {
    [K in keyof TRow]-?: NonNullable<TRow[K]> extends number | bigint | BigDecimal.BigDecimal
      ? K
      : never;
  }[keyof TRow],
  string
>;
export type StringField<TRow> = Extract<
  {
    [K in keyof TRow]-?: NonNullable<TRow[K]> extends string ? K : never;
  }[keyof TRow],
  string
>;
export type BooleanField<TRow> = Extract<
  {
    [K in keyof TRow]-?: NonNullable<TRow[K]> extends boolean ? K : never;
  }[keyof TRow],
  string
>;
export type ComparableField<TRow> = Extract<
  {
    [K in keyof TRow]-?: NonNullable<TRow[K]> extends
      | string
      | number
      | bigint
      | boolean
      | BigDecimal.BigDecimal
      ? K
      : never;
  }[keyof TRow],
  string
>;

export type NumberComparator =
  | "equals"
  | "not_equals"
  | "greater_than"
  | "greater_than_or_equal"
  | "less_than"
  | "less_than_or_equal"
  | "one_of";
export type StringComparator = "equals" | "not_equals" | "contains" | "starts_with" | "one_of";
export type BooleanComparator = "equals" | "not_equals" | "one_of";

type FilterableScalar<TValue> = NonNullable<TValue>;
type FilterStringValue<TValue extends string> = string extends TValue ? string : TValue;
type FilterScalarValue<TValue> =
  FilterableScalar<TValue> extends string
    ? FilterStringValue<FilterableScalar<TValue>>
    : FilterableScalar<TValue>;

export type Comparator<TValue> =
  FilterableScalar<TValue> extends number | bigint | BigDecimal.BigDecimal
    ? NumberComparator
    : FilterableScalar<TValue> extends string
      ? StringComparator
      : FilterableScalar<TValue> extends boolean
        ? BooleanComparator
        : never;

export type ComparatorValue<TValue, TComparator> = TComparator extends "one_of"
  ? readonly FilterScalarValue<TValue>[]
  : TComparator extends "contains" | "starts_with"
    ? string
    : FilterScalarValue<TValue>;

export type FieldPredicate<TRow, TField extends FieldOf<TRow>> = {
  [TComparator in Comparator<TRow[TField]>]: {
    readonly field: TField;
    readonly comparator: TComparator;
    readonly value: ComparatorValue<TRow[TField], TComparator>;
  };
}[Comparator<TRow[TField]>];

export type Predicate<TRow> = {
  [TField in FieldOf<TRow>]-?: FieldPredicate<TRow, TField>;
}[FieldOf<TRow>];

export type FilterNode<TRow> =
  | Predicate<TRow>
  | {
      readonly op: "and" | "or";
      readonly conditions: readonly FilterNode<TRow>[];
    };

export type FieldProjection<TRow> = Partial<Record<FieldOf<TRow>, true>>;
type SelectedFieldKeys<TFields> = Extract<
  {
    [K in keyof TFields]-?: TFields[K] extends true ? K : never;
  }[keyof TFields],
  string
>;

export type InferRawResult<TRow, TFields> = Pick<
  TRow,
  Extract<SelectedFieldKeys<TFields>, keyof TRow>
>;

export type AggregateDefinition<TRow> =
  | {
      readonly aggFunc: "count";
      readonly field: FieldOf<TRow>;
    }
  | {
      readonly aggFunc: "count_distinct";
      readonly field: FieldOf<TRow>;
    }
  | {
      readonly aggFunc: "sum" | "avg";
      readonly field: NumericField<TRow>;
    }
  | {
      readonly aggFunc: "min" | "max";
      readonly field: ComparableField<TRow>;
    }
  | {
      readonly aggFunc: "string_concat" | "string_concat_distinct";
      readonly field: FieldOf<TRow>;
      readonly joiner: string;
      readonly sort?: SortDirection | undefined;
    };

export type AggregateMap<TRow> = Record<string, AggregateDefinition<TRow>>;

type AggregateOperation<TAggregate> = TAggregate extends { readonly aggFunc: infer TOp }
  ? TOp
  : never;

type NumericAggregateOutput<
  TRow,
  TAggregate extends AggregateDefinition<TRow>,
> = TAggregate extends { readonly field: infer TField extends keyof TRow }
  ? NonNullable<TRow[TField]> extends BigDecimal.BigDecimal
    ? BigDecimal.BigDecimal
    : number
  : number;

type AggregateOutput<TRow, TAggregate extends AggregateDefinition<TRow>> =
  AggregateOperation<TAggregate> extends "count" | "count_distinct" | "sum" | "avg"
    ? AggregateOperation<TAggregate> extends "sum" | "avg"
      ? NumericAggregateOutput<TRow, TAggregate>
      : number
    : AggregateOperation<TAggregate> extends "string_concat" | "string_concat_distinct"
      ? string
      : TAggregate extends { readonly field: infer TField extends keyof TRow }
        ? TRow[TField]
        : never;

type InferAggregateOutputs<TRow, TAggregates extends AggregateMap<TRow>> = {
  readonly [TAggregateKey in keyof TAggregates]: AggregateOutput<TRow, TAggregates[TAggregateKey]>;
};

export type GroupByFields<TRow> = readonly [FieldOf<TRow>, ...FieldOf<TRow>[]];

export type InferGroupedResult<
  TRow,
  TGroupBy extends readonly FieldOf<TRow>[],
  TAggregates extends AggregateMap<TRow>,
> = Pick<TRow, Extract<TGroupBy[number], keyof TRow>> & InferAggregateOutputs<TRow, TAggregates>;

export type OrderBy<TRow> = readonly {
  readonly field: FieldOf<TRow>;
  readonly direction: SortDirection;
}[];

export type OrderByGrouped<
  TRow,
  TGroupBy extends readonly FieldOf<TRow>[],
  TAggregates extends AggregateMap<TRow>,
> = readonly {
  readonly field: TGroupBy[number] | Extract<keyof TAggregates, string>;
  readonly direction: SortDirection;
}[];

type QueryBase<TRow> = {
  readonly where?: FilterNode<TRow> | undefined;
  readonly limit?: number | undefined;
  readonly offset?: number | undefined;
};

export type RawQuery<
  TRow,
  TFields extends FieldProjection<TRow> = FieldProjection<TRow>,
> = QueryBase<TRow> & {
  readonly fields: TFields;
  readonly groupBy?: never;
  readonly aggregates?: never;
  readonly orderBy?: OrderBy<TRow> | undefined;
};

export type GroupedQuery<
  TRow,
  TGroupBy extends GroupByFields<TRow> = GroupByFields<TRow>,
  TAggregates extends AggregateMap<TRow> = AggregateMap<TRow>,
> = QueryBase<TRow> & {
  readonly fields?: never;
  readonly groupBy: TGroupBy;
  readonly aggregates: TAggregates;
  readonly orderBy?: OrderByGrouped<TRow, TGroupBy, TAggregates> | undefined;
};

export type Query<TRow> = RawQuery<TRow> | GroupedQuery<TRow>;

export type QueryForTopic<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = Query<TopicRowFromConfig<TConfig, TTopic>>;

export type QueryForReadableTopic<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
> = Query<ReadableTopicRowFromConfig<TConfig, TTopic>>;

export type InferredResult<TRow, TQuery extends Query<TRow>> =
  TQuery extends RawQuery<TRow, infer TFields>
    ? Array<InferRawResult<TRow, TFields>>
    : TQuery extends GroupedQuery<
          TRow,
          infer TGroupBy extends GroupByFields<TRow>,
          infer TAggregates
        >
      ? Array<InferGroupedResult<TRow, TGroupBy, TAggregates>>
      : never;

export type InferQueryResult<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
  TQuery extends QueryForTopic<TConfig, TTopic>,
> = InferredResult<TopicRowFromConfig<TConfig, TTopic>, TQuery>;

export type InferReadableQueryResult<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
> = InferredResult<ReadableTopicRowFromConfig<TConfig, TTopic>, TQuery>;

export type RuntimeRow = Record<string, unknown>;
export type RuntimeComparator = NumberComparator | StringComparator | BooleanComparator;
export type RuntimeAggregateDefinition =
  | {
      readonly aggFunc: "count" | "count_distinct" | "sum" | "avg" | "min" | "max";
      readonly field: string;
    }
  | {
      readonly aggFunc: "string_concat" | "string_concat_distinct";
      readonly field: string;
      readonly joiner: string;
      readonly sort?: SortDirection | undefined;
    };
export type RuntimeAggregateMap = Record<string, RuntimeAggregateDefinition>;
export type RuntimeFilterNode =
  | {
      readonly field: string;
      readonly comparator: RuntimeComparator;
      readonly value: unknown;
    }
  | {
      readonly op: "and" | "or";
      readonly conditions: readonly RuntimeFilterNode[];
    };
export type RuntimeRawQuery = {
  readonly fields: Readonly<Record<string, true>>;
  readonly where?: RuntimeFilterNode | undefined;
  readonly orderBy?: OrderBy<RuntimeRow> | undefined;
  readonly offset?: number | undefined;
  readonly limit?: number | undefined;
};
export type RuntimeGroupedQuery = {
  readonly groupBy: readonly string[];
  readonly aggregates: RuntimeAggregateMap;
  readonly where?: RuntimeFilterNode | undefined;
  readonly orderBy?:
    | readonly {
        readonly field: string;
        readonly direction: SortDirection;
      }[]
    | undefined;
  readonly offset?: number | undefined;
  readonly limit?: number | undefined;
};
export type RuntimeQuery = RuntimeRawQuery | RuntimeGroupedQuery;

export type RuntimeRowKey = string | number;
export type RuntimeRowKeyFn = (row: RuntimeRow) => RuntimeRowKey;

export function rowKeyByField(row: RuntimeRow, idField: string): RuntimeRowKey {
  const value = row[idField];
  return typeof value === "string" || typeof value === "number" ? value : stableStringify(value);
}

export function groupRowKey(row: RuntimeRow, groupBy: readonly string[]): string {
  return stableStringify(Object.fromEntries(groupBy.map((field) => [field, row[field]])));
}

export function rowKeyForQuery(query: RuntimeQuery, idField: string): RuntimeRowKeyFn {
  if (isRuntimeGroupedQuery(query)) {
    return (row) => groupRowKey(row, query.groupBy);
  }
  return (row) => rowKeyByField(row, idField);
}

export function isRuntimeGroupedQuery(query: RuntimeQuery): query is RuntimeGroupedQuery {
  return "groupBy" in query && Array.isArray(query.groupBy);
}

export function stableStringify(value: unknown): string {
  if (BigDecimal.isBigDecimal(value)) {
    return BigDecimal.format(value);
  }
  if (typeof value === "bigint") {
    return `${value.toString()}n`;
  }
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  if (
    value === null ||
    value === undefined ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  if (typeof value === "object") {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`)
      .join(",")}}`;
  }
  if (typeof value === "symbol") {
    return value.description === undefined ? "Symbol()" : `Symbol(${value.description})`;
  }
  if (typeof value === "function") {
    return `[Function:${value.name}]`;
  }
  return "unknown";
}

export type QueryResponse<TRow extends readonly unknown[]> = {
  readonly rows: TRow;
  readonly totalRows: number;
  readonly version: string;
};

export type SnapshotEvent<TRow extends readonly unknown[]> = {
  readonly type: "snapshot";
  readonly requestId: string;
  readonly rows: TRow;
  readonly meta: {
    readonly version: string;
    readonly totalRows: number;
    readonly backendVersion?: string | undefined;
    readonly serverTime: number;
  };
};

export type DeltaOperation<TRow> =
  | {
      readonly type: "upsert";
      readonly row: TRow;
      readonly key?: RuntimeRowKey | undefined;
      readonly index?: number | undefined;
    }
  | {
      readonly type: "patch";
      readonly key: string | number;
      readonly changes: Partial<TRow>;
      readonly index?: number | undefined;
    }
  | {
      readonly type: "remove";
      readonly key: string | number;
    };

export type DeltaEvent<TRow extends readonly unknown[]> = {
  readonly type: "delta";
  readonly requestId: string;
  readonly ops: readonly DeltaOperation<TRow[number]>[];
  readonly meta: {
    readonly fromVersion: string;
    readonly toVersion: string;
    readonly totalRows: number;
    readonly sourceUpdatedAt?: number | bigint | undefined;
    readonly serverTime: number;
  };
};

export type SubscriptionEvent<TRow extends readonly unknown[]> =
  | SnapshotEvent<TRow>
  | DeltaEvent<TRow>;

export type UseSubscriptionResult<TData extends readonly unknown[]> = {
  readonly data: TData;
  readonly totalRows: number;
  readonly status: SubscriptionStatus;
  readonly error?: unknown;
};
