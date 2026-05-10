import { Effect } from "effect";
import type {
  EffectSourceContext,
  IdValue,
  KafkaSourceConfig,
  KafkaSourceMessage,
  RowObject,
} from "../config/index.ts";
import { kafkaIngestFailed, type ViewServerError } from "../errors.ts";
import type { KafkaRecordBatch, KafkaTopicConsumer } from "./types.ts";

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
}): Effect.Effect<void, ViewServerError> {
  const commitPolicy = args.source.commitPolicy ?? "after-ingest";
  return args.consumer.run({
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
      }),
  });
}

export function ingestKafkaBatch<TRow extends RowObject, TId extends keyof TRow & string>(args: {
  readonly viewTopic: string;
  readonly idField: TId;
  readonly source: KafkaSourceConfig<TRow, TId>;
  readonly runtime: KafkaSourceRuntime<TRow, TId>;
  readonly batch: KafkaRecordBatch;
  readonly commitPolicy: "after-ingest" | "none";
}): Effect.Effect<void, ViewServerError> {
  return Effect.forEach(
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
  ).pipe(Effect.andThen(args.commitPolicy === "after-ingest" ? args.batch.commit : Effect.void));
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
