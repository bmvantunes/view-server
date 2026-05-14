import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { invalidStartupEnv, type InvalidStartupEnv } from "../errors.ts";

const ViewServerPort = Schema.NumberFromString.pipe(
  Schema.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 65_535 })),
);

export const RawViewServerStartupEnv = Schema.Struct({
  KAFKA_BROKERS: Schema.NonEmptyString,
  VIEW_SERVER_PORT: ViewServerPort,
  VIEW_SERVER_RPC_PATH: Schema.NonEmptyString,
});

export type RawViewServerStartupEnv = typeof RawViewServerStartupEnv.Type;

export type ViewServerStartupEnv = {
  readonly kafkaBrokers: readonly [string, ...string[]];
  readonly rpcPort: number;
  readonly rpcPath: `/${string}`;
};

export class ViewServerStartupEnvService extends Context.Service<
  ViewServerStartupEnvService,
  ViewServerStartupEnv
>()("@view-server/core/ViewServerStartupEnv") {}

export function decodeViewServerStartupEnv(
  env: Record<string, string | undefined>,
): Effect.Effect<ViewServerStartupEnv, InvalidStartupEnv> {
  return Effect.fn("view-server.env.decode")(function* () {
    const raw = yield* Schema.decodeUnknownEffect(RawViewServerStartupEnv)(env).pipe(
      Effect.mapError((error) => invalidStartupEnv("Invalid startup environment", error)),
    );
    const kafkaBrokers = splitKafkaBrokers(raw.KAFKA_BROKERS);
    if (kafkaBrokers === undefined) {
      return yield* Effect.fail(
        invalidStartupEnv(
          "KAFKA_BROKERS must contain at least one broker",
          undefined,
          "KAFKA_BROKERS",
        ),
      );
    }
    const rpcPath = raw.VIEW_SERVER_RPC_PATH;
    if (!isRpcPath(rpcPath)) {
      return yield* Effect.fail(
        invalidStartupEnv(
          "VIEW_SERVER_RPC_PATH must start with /",
          undefined,
          "VIEW_SERVER_RPC_PATH",
        ),
      );
    }
    return {
      kafkaBrokers,
      rpcPort: raw.VIEW_SERVER_PORT,
      rpcPath,
    };
  })();
}

export const layerViewServerStartupEnv = (
  env: Record<string, string | undefined>,
): Layer.Layer<ViewServerStartupEnvService, InvalidStartupEnv> =>
  Layer.effect(ViewServerStartupEnvService, decodeViewServerStartupEnv(env));

function splitKafkaBrokers(value: string): readonly [string, ...string[]] | undefined {
  const brokers = value
    .split(",")
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);
  const first = brokers[0];
  return first === undefined ? undefined : [first, ...brokers.slice(1)];
}

function isRpcPath(value: string): value is `/${string}` {
  return value.startsWith("/");
}
