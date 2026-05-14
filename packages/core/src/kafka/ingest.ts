import * as Effect from "effect/Effect";
import type {
  EffectSourceContext,
  IdValue,
  KafkaConsumerRecord,
  KafkaSourceConfig,
  KafkaSourceMessage,
  RowObject,
} from "../config/index.ts";
import { kafkaIngestFailed, type ViewServerError } from "../errors.ts";
import type { KafkaBatchMetrics, KafkaRecordBatch, KafkaTopicConsumer } from "./types.ts";

export type KafkaSourceRuntime<TRow extends RowObject, TId extends keyof TRow & string> = Pick<
  EffectSourceContext<TRow, TId>,
  "publish" | "deltaPublish" | "deleteById"
>;

export function runKafkaSource<TRow extends RowObject, TId extends keyof TRow & string>(args: {
  readonly viewTopic: string;
  readonly idField: TId;
  readonly source: KafkaSourceConfig<TRow, TId>;
  readonly consumer: KafkaTopicConsumer;
  readonly runtime: KafkaSourceRuntime<TRow, TId>;
  readonly onBatchMetrics?:
    | ((metrics: KafkaBatchMetrics) => Effect.Effect<void, ViewServerError>)
    | undefined;
}): Effect.Effect<void, ViewServerError> {
  return Effect.fn("view-server.kafka.source.run")(function* () {
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": args.viewTopic,
    });
    const commitPolicy = args.source.commitPolicy ?? "after-ingest";
    yield* args.consumer.run({
      topic: args.source.topic,
      groupId: args.source.groupId,
      commitPolicy,
      onBatch: (batch) =>
        ingestKafkaBatch({
          viewTopic: args.viewTopic,
          idField: args.idField,
          source: args.source,
          runtime: args.runtime,
          batch,
          commitPolicy,
          onBatchMetrics: args.onBatchMetrics,
        }),
    });
  })();
}

export function ingestKafkaBatch<TRow extends RowObject, TId extends keyof TRow & string>(args: {
  readonly viewTopic: string;
  readonly idField: TId;
  readonly source: KafkaSourceConfig<TRow, TId>;
  readonly runtime: KafkaSourceRuntime<TRow, TId>;
  readonly batch: KafkaRecordBatch;
  readonly commitPolicy: "after-ingest" | "none";
  readonly onBatchMetrics?:
    | ((metrics: KafkaBatchMetrics) => Effect.Effect<void, ViewServerError>)
    | undefined;
}): Effect.Effect<void, ViewServerError> {
  return Effect.fn("view-server.kafka.batch.ingest")(function* () {
    const lastRecord = args.batch.records[args.batch.records.length - 1];
    const metrics = kafkaBatchMetrics(args.batch);
    const lag = metrics?.lagMax ?? args.batch.lag ?? kafkaRecordLag(lastRecord);
    yield* Effect.annotateCurrentSpan({
      "view_server.topic": args.viewTopic,
      "view_server.batch_size": args.batch.records.length,
      ...(lag === undefined ? {} : { "view_server.kafka.lag": lag }),
      ...(lastRecord === undefined
        ? {}
        : {
            "view_server.kafka.partition": lastRecord.partition,
            "view_server.kafka.offset": lastRecord.offset,
          }),
    });
    yield* Effect.forEach(
      args.batch.records,
      (record) =>
        args.source
          .decode(record)
          .pipe(
            Effect.flatMap((message) =>
              applyKafkaSourceMessage(args.viewTopic, args.idField, args.runtime, message),
            ),
          ),
      { discard: true },
    );
    if (metrics !== undefined && args.onBatchMetrics !== undefined) {
      yield* args.onBatchMetrics(metrics);
    }
    if (args.commitPolicy === "after-ingest") {
      yield* args.batch.commit;
    }
  })();
}

export function kafkaBatchMetrics(batch: KafkaRecordBatch): KafkaBatchMetrics | undefined {
  if (batch.metrics !== undefined) {
    return batch.metrics;
  }
  const lastRecord = batch.records[batch.records.length - 1];
  if (lastRecord === undefined && batch.lag === undefined) {
    return undefined;
  }
  const offset = kafkaOffsetNumber(lastRecord?.offset);
  const endOffset = kafkaOffsetNumber(lastRecord?.highWatermark ?? lastRecord?.endOffset);
  const lag = batch.lag ?? kafkaRecordLag(lastRecord) ?? 0;
  return {
    lagTotal: lag,
    lagMax: lag,
    partitions: lastRecord?.partition === undefined ? 0 : 1,
    ...(offset === undefined ? {} : { offset }),
    ...(endOffset === undefined ? {} : { endOffset }),
  };
}

export function kafkaRecordLag(record: KafkaConsumerRecord | undefined): number | undefined {
  if (record === undefined) {
    return undefined;
  }
  const endOffset = parseKafkaOffset(record.highWatermark ?? record.endOffset);
  const offset = parseKafkaOffset(record.offset);
  if (endOffset === undefined || offset === undefined) {
    return undefined;
  }
  return spanSafeNumber(endOffset - offset - 1n);
}

export function applyKafkaSourceMessage<TRow extends RowObject, TId extends keyof TRow & string>(
  topic: string,
  idField: TId,
  runtime: KafkaSourceRuntime<TRow, TId>,
  message: KafkaSourceMessage<TRow, TId>,
): Effect.Effect<void, ViewServerError> {
  if (isPublishEnvelope(message)) {
    return runtime.publish(message.row);
  }
  if (isDeltaPublishEnvelope(message)) {
    return runtime.deltaPublish(message.patch);
  }
  if (isDeleteEnvelope(message)) {
    return runtime.deleteById(message.id);
  }
  if (!hasId(message, idField)) {
    return Effect.fail(
      kafkaIngestFailed(topic, new Error(`Decoded Kafka row is missing ${idField}`)),
    );
  }
  return runtime.publish(message);
}

function parseKafkaOffset(offset: string | number | undefined): bigint | undefined {
  if (offset === undefined) {
    return undefined;
  }
  if (typeof offset === "number") {
    return Number.isSafeInteger(offset) ? BigInt(offset) : undefined;
  }
  try {
    return BigInt(offset);
  } catch {
    return undefined;
  }
}

function kafkaOffsetNumber(offset: string | number | undefined): number | undefined {
  const value = parseKafkaOffset(offset);
  return value === undefined ? undefined : spanSafeNumber(value);
}

function spanSafeNumber(value: bigint): number {
  if (value <= 0n) {
    return 0;
  }
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number.MAX_SAFE_INTEGER;
  }
  return Number(value);
}

function isPublishEnvelope<TRow extends RowObject, TId extends keyof TRow & string>(
  message: KafkaSourceMessage<TRow, TId>,
): message is { readonly type: "publish"; readonly row: TRow } {
  return isObject(message) && message.type === "publish" && "row" in message;
}

function isDeltaPublishEnvelope<TRow extends RowObject, TId extends keyof TRow & string>(
  message: KafkaSourceMessage<TRow, TId>,
): message is { readonly type: "delta-publish"; readonly patch: Partial<TRow> & Pick<TRow, TId> } {
  return isObject(message) && message.type === "delta-publish" && "patch" in message;
}

function isDeleteEnvelope<TRow extends RowObject, TId extends keyof TRow & string>(
  message: KafkaSourceMessage<TRow, TId>,
): message is { readonly type: "delete"; readonly id: Extract<TRow[TId], IdValue> } {
  return isObject(message) && message.type === "delete" && "id" in message;
}

function hasId<TRow extends RowObject, TId extends keyof TRow & string>(
  message: KafkaSourceMessage<TRow, TId>,
  idField: TId,
): message is TRow {
  return isObject(message) && idField in message;
}

function isObject(value: unknown): value is RowObject {
  return typeof value === "object" && value !== null;
}
