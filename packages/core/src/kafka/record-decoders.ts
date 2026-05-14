import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type {
  IdValue,
  KafkaConsumerRecord,
  KafkaSourceMessage,
  RowObject,
} from "../config/index.ts";
import { kafkaIngestFailed, schemaDecodeFailed, type ViewServerError } from "../errors.ts";

export function decodeJsonRecord<TRow extends RowObject, TId extends keyof TRow & string>(args: {
  readonly topic: string;
  readonly schema: Schema.Decoder<TRow, never>;
}): (record: KafkaConsumerRecord) => Effect.Effect<KafkaSourceMessage<TRow, TId>, ViewServerError> {
  return (record) =>
    decodeKafkaRecordJson(args.topic, record).pipe(
      Effect.flatMap((value) =>
        isMutationEnvelope<TRow, TId>(value)
          ? decodeEnvelope(args.topic, args.schema, value)
          : Schema.decodeUnknownEffect(args.schema)(value).pipe(
              Effect.mapError((error) => schemaDecodeFailed(args.topic, error)),
            ),
      ),
    );
}

export function decodeKafkaRecordJson(
  topic: string,
  record: KafkaConsumerRecord,
): Effect.Effect<unknown, ViewServerError> {
  return Effect.try({
    try: () => {
      if (record.value === null) {
        throw new Error("Kafka record value is null");
      }
      const text =
        typeof record.value === "string" ? record.value : new TextDecoder().decode(record.value);
      return JSON.parse(text);
    },
    catch: (error) => kafkaIngestFailed(topic, error),
  });
}

export type ProtobufDecimalInput =
  | string
  | BigDecimal.BigDecimal
  | {
      readonly value: string;
    }
  | {
      readonly units: string | bigint;
      readonly scale: number;
    }
  | {
      readonly unscaled: string | bigint | Uint8Array;
      readonly scale: number;
    }
  | {
      readonly unscaledValue: string | bigint | Uint8Array;
      readonly scale: number;
    }
  | {
      readonly bytes: Uint8Array;
      readonly scale: number;
    };

export function decodeProtobufDecimal(
  topic: string,
  input: ProtobufDecimalInput,
): Effect.Effect<BigDecimal.BigDecimal, ViewServerError> {
  return Effect.try({
    try: () => protobufDecimalToBigDecimal(input),
    catch: (error) => schemaDecodeFailed(topic, error),
  });
}

export function protobufDecimalToBigDecimal(input: ProtobufDecimalInput): BigDecimal.BigDecimal {
  if (BigDecimal.isBigDecimal(input)) {
    return input;
  }
  if (typeof input === "string") {
    return BigDecimal.fromStringUnsafe(input);
  }
  if ("value" in input) {
    return BigDecimal.fromStringUnsafe(input.value);
  }
  if ("units" in input) {
    return unscaledDecimalToBigDecimal(input.units, input.scale);
  }
  if ("unscaled" in input) {
    return unscaledDecimalToBigDecimal(input.unscaled, input.scale);
  }
  if ("unscaledValue" in input) {
    return unscaledDecimalToBigDecimal(input.unscaledValue, input.scale);
  }
  return unscaledDecimalToBigDecimal(input.bytes, input.scale);
}

export function unscaledDecimalToBigDecimal(
  unscaled: string | bigint | Uint8Array,
  scale: number,
): BigDecimal.BigDecimal {
  if (!Number.isSafeInteger(scale)) {
    throw new Error(`Invalid decimal scale: ${scale}`);
  }
  const value =
    typeof unscaled === "bigint"
      ? unscaled
      : typeof unscaled === "string"
        ? BigInt(unscaled)
        : signedBigIntFromTwosComplement(unscaled);
  return BigDecimal.make(value, scale);
}

function signedBigIntFromTwosComplement(bytes: Uint8Array): bigint {
  if (bytes.length === 0) {
    return 0n;
  }
  let value = 0n;
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte);
  }
  return bytes[0]! & 0x80 ? value - (1n << BigInt(bytes.length * 8)) : value;
}

type MutationEnvelope<TRow extends RowObject, TId extends keyof TRow & string> =
  | { readonly type: "publish"; readonly row: unknown }
  | { readonly type: "delta-publish"; readonly patch: Partial<TRow> & Pick<TRow, TId> }
  | { readonly type: "delete"; readonly id: Extract<TRow[TId], IdValue> };

function decodeEnvelope<TRow extends RowObject, TId extends keyof TRow & string>(
  topic: string,
  schema: Schema.Decoder<TRow, never>,
  envelope: MutationEnvelope<TRow, TId>,
): Effect.Effect<KafkaSourceMessage<TRow, TId>, ViewServerError> {
  if (envelope.type === "delete") {
    return Effect.succeed(envelope);
  }
  if (envelope.type === "delta-publish") {
    return Effect.succeed(envelope);
  }
  return Schema.decodeUnknownEffect(schema)(envelope.row).pipe(
    Effect.map((row) => ({ type: "publish" as const, row })),
    Effect.mapError((error) => schemaDecodeFailed(topic, error)),
  );
}

function isMutationEnvelope<TRow extends RowObject, TId extends keyof TRow & string>(
  value: unknown,
): value is MutationEnvelope<TRow, TId> {
  if (typeof value !== "object" || value === null || !("type" in value)) {
    return false;
  }
  if (value.type === "publish") {
    return "row" in value;
  }
  if (value.type === "delta-publish") {
    return "patch" in value && typeof value.patch === "object" && value.patch !== null;
  }
  return (
    value.type === "delete" &&
    "id" in value &&
    (typeof value.id === "string" || typeof value.id === "number")
  );
}
