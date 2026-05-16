import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type {
  ReadableTopicName,
  TopicName,
  TopicPatchFromConfig,
  TopicRowFromConfig,
  ViewServerConfig,
} from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC, ViewServerHealthRowSchema } from "../config/index.ts";
import { schemaDecodeFailed, type ViewServerError } from "../errors.ts";
import {
  rowKeyForQuery,
  type DeltaOperation,
  type InferReadableQueryResult,
  type QueryForReadableTopic,
  type RuntimeAggregateDefinition,
  type RuntimeQuery,
  type RuntimeRow,
  type SubscriptionEvent,
} from "../protocol/index.ts";
import { rowKeyFromTopicConfig } from "../protocol/row-key.ts";
import {
  fromWireRow,
  fromWireRows,
  toWireRow,
  type RpcDeltaPublishPayload,
  type RpcPublishPayload,
  type RpcQueryPayload,
  type RpcQueryResponse,
  type RpcSubscribePayload,
  type RpcSubscriptionEvent,
} from "../rpc/index.ts";

type RpcDeltaOperation = Extract<RpcSubscriptionEvent, { readonly type: "delta" }>["ops"][number];

export function rpcQueryPayload<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(topic: TTopic, query: TQuery): RpcQueryPayload {
  return {
    topic,
    query: toRpcQuery(query),
  };
}

export function rpcSubscribePayload<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(requestId: string, topic: TTopic, query: TQuery): RpcSubscribePayload {
  return {
    requestId,
    topic,
    query: toRpcQuery(query),
  };
}

export function rpcPublishPayload<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
>(
  config: TConfig,
  topic: TTopic,
  row: TopicRowFromConfig<TConfig, TTopic>,
): Effect.Effect<RpcPublishPayload, ViewServerError> {
  return Schema.decodeUnknownEffect(config.topics[topic].schema)(row).pipe(
    Effect.map((decodedRow) => ({
      topic,
      row: toWireRow(decodedRow),
    })),
    Effect.mapError((error) => schemaDecodeFailed(String(topic), error)),
  );
}

export function rpcDeltaPublishPayload<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
>(
  config: TConfig,
  topic: TTopic,
  patch: TopicPatchFromConfig<TConfig, TTopic>,
): Effect.Effect<RpcDeltaPublishPayload, ViewServerError> {
  const patchSchema = Schema.declare<TopicPatchFromConfig<TConfig, TTopic>>((input) =>
    isTopicPatch(input, config, topic),
  );
  return Schema.decodeUnknownEffect(patchSchema)(patch).pipe(
    Effect.map((decodedPatch) => ({
      topic,
      patch: toWireRow(decodedPatch),
    })),
    Effect.mapError((error) => schemaDecodeFailed(String(topic), error)),
  );
}

export function rpcQueryRows<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  response: RpcQueryResponse,
  query: TQuery,
  config: TConfig,
  topic: TTopic,
): Effect.Effect<InferReadableQueryResult<TConfig, TTopic, TQuery>, ViewServerError> {
  const rows = fromWireRows(response.rows);
  return Schema.decodeUnknownEffect(queryResultSchema(config, topic, query))(rows).pipe(
    Effect.mapError((error) => schemaDecodeFailed(String(topic), error)),
  );
}

export function rpcSubscriptionEvent<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  event: RpcSubscriptionEvent,
  query: TQuery,
  config: TConfig,
  topic: TTopic,
): Effect.Effect<SubscriptionEvent<readonly RuntimeRow[]>, ViewServerError> {
  if (event.type === "snapshot") {
    return rpcQueryRows<TConfig, TTopic, TQuery>(
      {
        rows: event.rows,
        totalRows: event.meta.totalRows,
        version: event.meta.version,
      },
      query,
      config,
      topic,
    ).pipe(
      Effect.map((rows) => ({
        type: "snapshot",
        requestId: event.requestId,
        rows: queryResultToRuntimeRows(rows),
        meta: event.meta,
      })),
    );
  }
  if (event.type === "status") {
    return Effect.succeed({
      type: "status",
      requestId: event.requestId,
      status: event.status,
      meta: event.meta,
    });
  }

  return Effect.forEach(event.ops, (operation) =>
    decodeDeltaOperation(operation, event.meta.toVersion, config, topic, query),
  ).pipe(
    Effect.map((ops) => ({
      type: "delta",
      requestId: event.requestId,
      ops,
      meta: event.meta,
    })),
  );
}

function decodeDeltaOperation<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  operation: RpcDeltaOperation,
  version: string,
  config: TConfig,
  topic: TTopic,
  query: TQuery,
): Effect.Effect<DeltaOperation<RuntimeRow>, ViewServerError> {
  const rowKey = rowKeyFromTopicConfig(config.topics[topic]);
  if (operation.type === "remove") {
    return Effect.succeed({
      type: "remove" as const,
      key: rowKey.decodeFromWire(operation.key),
    });
  }
  if (operation.type === "patch") {
    const changes = fromWireRow(operation.changes);
    const key = rowKey.decodeFromWire(operation.key);
    return decodeResultPatch(changes, config, topic, query).pipe(
      Effect.map((decodedChanges) => ({
        type: "patch" as const,
        key,
        changes: decodedChanges,
        ...(operation.index === undefined ? {} : { index: operation.index }),
      })),
    );
  }
  return rpcQueryRows<TConfig, TTopic, TQuery>(
    {
      rows: [operation.row],
      totalRows: 1,
      version,
    },
    query,
    config,
    topic,
  ).pipe(
    Effect.flatMap((rows) => {
      const [row] = queryResultToRuntimeRows(rows);
      return row === undefined
        ? Effect.fail(schemaDecodeFailed(String(topic), "Delta upsert did not contain a row"))
        : Effect.succeed({
            type: "upsert" as const,
            row,
            ...(operation.key === undefined ? {} : { key: rowKey.decodeFromWire(operation.key) }),
            ...(operation.index === undefined ? {} : { index: operation.index }),
          });
    }),
  );
}

export function queryResultToRuntimeRows(
  data: readonly object[] | undefined,
): readonly RuntimeRow[] {
  if (data === undefined) {
    return [];
  }
  return data.map((row) => Object.fromEntries(Object.entries(row)));
}

export function runtimeRowsToQueryResult<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  rows: readonly RuntimeRow[],
  query: TQuery,
  config: TConfig,
  topic: TTopic,
): InferReadableQueryResult<TConfig, TTopic, TQuery> {
  return Schema.decodeUnknownSync(queryResultSchema(config, topic, query))(rows);
}

export function rowKeyForTypedQuery<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(query: TQuery, idField: string) {
  const runtimeQuery = toRuntimeQuery(query);
  return rowKeyForQuery(runtimeQuery, idField);
}

function toRpcQuery(query: unknown): RpcQueryPayload["query"] {
  if (isRpcQuery(query)) {
    return query;
  }
  throw new Error("Invalid RPC query shape");
}

function toRuntimeQuery(query: unknown): RuntimeQuery {
  if (isRuntimeQuery(query)) {
    return query;
  }
  throw new Error(`Invalid RPC query shape: ${describeUnknown(query)}`);
}

function queryResultSchema<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(config: TConfig, topic: TTopic, query: TQuery) {
  return Schema.declare<InferReadableQueryResult<TConfig, TTopic, TQuery>>((input) =>
    isReadableQueryResultRows<TConfig, TTopic, TQuery>(input, query, config, topic),
  );
}

function isReadableQueryResultRows<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  rows: unknown,
  query: TQuery,
  config: TConfig,
  topic: TTopic,
): rows is InferReadableQueryResult<TConfig, TTopic, TQuery> {
  if (!Array.isArray(rows)) {
    return false;
  }
  const runtimeQuery = toRuntimeQuery(query);
  const fields = resultFieldsForQuery(config, topic, runtimeQuery);
  return rows.every((row) => isResultRow(row, fields));
}

function decodeResultPatch<
  TConfig extends ViewServerConfig,
  TTopic extends ReadableTopicName<TConfig>,
  TQuery extends QueryForReadableTopic<TConfig, TTopic>,
>(
  patch: RuntimeRow,
  config: TConfig,
  topic: TTopic,
  query: TQuery,
): Effect.Effect<RuntimeRow, ViewServerError> {
  const patchSchema = Schema.declare<RuntimeRow>((input): input is RuntimeRow => {
    if (!isRecord(input)) {
      return false;
    }
    const fields = resultFieldsForQuery(config, topic, toRuntimeQuery(query));
    return Object.entries(input).every(([field, value]) =>
      validateResultFieldValue(fields, field, value),
    );
  });
  return Schema.decodeUnknownEffect(patchSchema)(patch).pipe(
    Effect.mapError((error) => schemaDecodeFailed(String(topic), error)),
  );
}

type ResultField = {
  readonly name: string;
  readonly required: boolean;
  readonly validate: (value: unknown) => boolean;
};

type FieldValidator = {
  readonly required: boolean;
  readonly validate: (value: unknown) => boolean;
};

function resultFieldsForQuery(
  config: ViewServerConfig,
  topic: string,
  query: RuntimeQuery,
): readonly ResultField[] {
  const fieldValidators = fieldValidatorsForReadableTopic(config, topic);
  if (isGroupedQuery(query)) {
    return [
      ...query.groupBy.map((field) => ({
        name: field,
        required: requiredForField(fieldValidators, field),
        validate: validatorForField(fieldValidators, field),
      })),
      ...Object.entries(query.aggregates).map(([alias, aggregate]) => ({
        name: alias,
        required: true,
        validate: validatorForAggregate(fieldValidators, aggregate),
      })),
    ];
  }

  const fields = new Set([idFieldForTopic(config, topic), ...Object.keys(query.fields)]);
  return Array.from(fields).map((field) => ({
    name: field,
    required: requiredForField(fieldValidators, field),
    validate: validatorForField(fieldValidators, field),
  }));
}

function isResultRow(row: unknown, fields: readonly ResultField[]): row is RuntimeRow {
  if (!isRecord(row)) {
    return false;
  }
  return fields.every((field) => {
    if (!Object.hasOwn(row, field.name)) {
      return !field.required;
    }
    return field.validate(row[field.name]);
  });
}

function validateResultFieldValue(
  fields: readonly ResultField[],
  field: string,
  value: unknown,
): boolean {
  const resultField = fields.find((candidate) => candidate.name === field);
  return resultField === undefined ? true : resultField.validate(value);
}

function validatorForField(
  fieldValidators: ReadonlyMap<string, FieldValidator>,
  field: string,
): (value: unknown) => boolean {
  return fieldValidators.get(field)?.validate ?? (() => true);
}

function requiredForField(
  fieldValidators: ReadonlyMap<string, FieldValidator>,
  field: string,
): boolean {
  return fieldValidators.get(field)?.required ?? true;
}

function validatorForAggregate(
  fieldValidators: ReadonlyMap<string, FieldValidator>,
  aggregate: RuntimeAggregateDefinition,
): (value: unknown) => boolean {
  switch (aggregate.aggFunc) {
    case "count":
    case "count_distinct":
      return (value) => typeof value === "number";
    case "sum":
    case "avg": {
      const fieldValidator = fieldValidators.get(aggregate.field);
      if (fieldValidator?.validate(BigDecimal.make(0n, 0)) === true) {
        return BigDecimal.isBigDecimal;
      }
      return (value) => typeof value === "number";
    }
    case "min":
    case "max":
      return validatorForField(fieldValidators, aggregate.field);
    case "string_concat":
    case "string_concat_distinct":
      return (value) => typeof value === "string";
  }
}

function fieldValidatorsForReadableTopic(
  config: ViewServerConfig,
  topic: string,
): ReadonlyMap<string, FieldValidator> {
  const schema =
    topic === VIEW_SERVER_HEALTH_TOPIC ? ViewServerHealthRowSchema : config.topics[topic]?.schema;
  if (!hasSchemaFields(schema)) {
    return new Map();
  }
  return new Map(
    Object.entries(schema.fields).map(([field, fieldSchema]) => [
      field,
      {
        required: !schemaAllowsUndefined(fieldSchema),
        validate: (value: unknown) => Schema.is(fieldSchema)(value),
      },
    ]),
  );
}

function isTopicPatch<TConfig extends ViewServerConfig, TTopic extends TopicName<TConfig>>(
  input: unknown,
  config: TConfig,
  topic: TTopic,
): input is TopicPatchFromConfig<TConfig, TTopic> {
  if (!isRecord(input) || !Object.hasOwn(input, config.topics[topic].id)) {
    return false;
  }
  const fieldValidators = fieldValidatorsForReadableTopic(config, String(topic));
  return Object.entries(input).every(([field, value]) =>
    validatorForField(fieldValidators, field)(value),
  );
}

function idFieldForTopic<TConfig extends ViewServerConfig>(config: TConfig, topic: string): string {
  return topic === VIEW_SERVER_HEALTH_TOPIC ? "id" : String(config.topics[topic]?.id ?? "id");
}

function isRuntimeQuery(value: unknown): value is RuntimeQuery {
  if (!isRecord(value)) {
    return false;
  }
  if (Array.isArray(value.groupBy) && isRecord(value.aggregates)) {
    return (
      value.groupBy.every((field) => typeof field === "string") &&
      Object.values(value.aggregates).every(isRuntimeAggregate)
    );
  }
  return isRecord(value.fields) && Object.values(value.fields).every((enabled) => enabled === true);
}

function isRpcQuery(value: unknown): value is RpcQueryPayload["query"] {
  if (!isRecord(value)) {
    return false;
  }
  const common =
    isRuntimeFilterNode(value.where) &&
    isOrderBy(value.orderBy) &&
    isOptionalFiniteNumber(value.offset) &&
    isOptionalFiniteNumber(value.limit);
  if (!common) {
    return false;
  }
  if (Array.isArray(value.groupBy) && isRecord(value.aggregates)) {
    return (
      value.groupBy.every((field) => typeof field === "string") &&
      Object.values(value.aggregates).every(isRuntimeAggregate)
    );
  }
  return isRecord(value.fields) && Object.values(value.fields).every((enabled) => enabled === true);
}

function isRuntimeFilterNode(value: unknown): boolean {
  if (value === undefined) {
    return true;
  }
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.op === "string") {
    return (
      (value.op === "and" || value.op === "or") &&
      Array.isArray(value.conditions) &&
      value.conditions.every(isRuntimeFilterNode)
    );
  }
  return (
    typeof value.field === "string" &&
    isRuntimeComparator(value.comparator) &&
    isRpcWireValue(value.value)
  );
}

function isRuntimeComparator(value: unknown): boolean {
  return (
    value === "equals" ||
    value === "not_equals" ||
    value === "greater_than" ||
    value === "greater_than_or_equal" ||
    value === "less_than" ||
    value === "less_than_or_equal" ||
    value === "contains" ||
    value === "starts_with" ||
    value === "one_of"
  );
}

function isOrderBy(value: unknown): boolean {
  return (
    value === undefined ||
    (Array.isArray(value) &&
      value.every(
        (order) =>
          isRecord(order) &&
          typeof order.field === "string" &&
          (order.direction === "asc" || order.direction === "desc"),
      ))
  );
}

function isOptionalFiniteNumber(value: unknown): boolean {
  return value === undefined || (typeof value === "number" && Number.isFinite(value));
}

function isRpcWireValue(value: unknown): boolean {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    BigDecimal.isBigDecimal(value)
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every(isRpcWireValue);
  }
  return isRecord(value) && Object.values(value).every(isRpcWireValue);
}

function isGroupedQuery(
  query: RuntimeQuery,
): query is Extract<RuntimeQuery, { readonly groupBy: readonly string[] }> {
  return "groupBy" in query && Array.isArray(query.groupBy);
}

function isRuntimeAggregate(value: unknown): boolean {
  if (!isRecord(value) || typeof value.field !== "string") {
    return false;
  }
  if (
    value.aggFunc === "count" ||
    value.aggFunc === "count_distinct" ||
    value.aggFunc === "sum" ||
    value.aggFunc === "avg" ||
    value.aggFunc === "min" ||
    value.aggFunc === "max"
  ) {
    return true;
  }
  return (
    (value.aggFunc === "string_concat" || value.aggFunc === "string_concat_distinct") &&
    typeof value.joiner === "string"
  );
}

function hasSchemaFields(value: unknown): value is {
  readonly fields: Readonly<Record<string, Schema.Top>>;
} {
  return isRecord(value) && isRecord(value.fields);
}

function schemaAllowsUndefined(schema: Schema.Top): boolean {
  return astAllowsUndefined(schema.ast);
}

function astAllowsUndefined(ast: unknown): boolean {
  if (!isRecord(ast)) {
    return false;
  }
  if (ast._tag === "Undefined") {
    return true;
  }
  return ast._tag === "Union" && Array.isArray(ast.types) && ast.types.some(astAllowsUndefined);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function describeUnknown(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
