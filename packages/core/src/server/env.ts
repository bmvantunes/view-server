import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { readViewServerConfigExport, type ViewServerConfig } from "../config/index.ts";
import {
  invalidConfig,
  invalidStartupEnv,
  type InvalidConfig,
  type InvalidStartupEnv,
} from "../errors.ts";

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

export const RawViewServerProductionEnv = Schema.Struct({
  KAFKA_BROKERS: Schema.NonEmptyString,
  VIEW_SERVER_PORT: ViewServerPort,
  VIEW_SERVER_RPC_PATH: Schema.NonEmptyString,
  VIEW_SERVER_CONFIG_MODULE: Schema.NonEmptyString,
  VIEW_SERVER_CONFIG_EXPORT: Schema.optional(Schema.NonEmptyString),
});

export type RawViewServerProductionEnv = typeof RawViewServerProductionEnv.Type;

export type ViewServerProductionEnv = ViewServerStartupEnv & {
  readonly configModuleUrl: string;
  readonly configExport?: string | undefined;
};

export type ViewServerProductionConfig = {
  readonly env: ViewServerProductionEnv;
  readonly config: ViewServerConfig;
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

export function decodeViewServerProductionEnv(
  env: Record<string, string | undefined>,
): Effect.Effect<ViewServerProductionEnv, InvalidStartupEnv> {
  return Effect.fn("view-server.env.production.decode")(function* () {
    const raw = yield* Schema.decodeUnknownEffect(RawViewServerProductionEnv)(env).pipe(
      Effect.mapError((error) => invalidStartupEnv("Invalid production environment", error)),
    );
    const startup = yield* decodeViewServerStartupEnv(env);
    return {
      ...startup,
      configModuleUrl: raw.VIEW_SERVER_CONFIG_MODULE,
      ...(raw.VIEW_SERVER_CONFIG_EXPORT === undefined
        ? {}
        : { configExport: raw.VIEW_SERVER_CONFIG_EXPORT }),
    };
  })();
}

export function loadViewServerProductionConfigFromEnv(
  env: Record<string, string | undefined>,
): Effect.Effect<ViewServerProductionConfig, InvalidStartupEnv | InvalidConfig> {
  return Effect.fn("view-server.env.production.load_config")(function* () {
    const productionEnv = yield* decodeViewServerProductionEnv(env);
    const configModuleUrl = yield* Effect.try({
      try: () => toImportUrl(productionEnv.configModuleUrl),
      catch: (error) =>
        invalidStartupEnv(
          "Failed to resolve VIEW_SERVER_CONFIG_MODULE",
          error,
          "VIEW_SERVER_CONFIG_MODULE",
        ),
    });
    const moduleValue = yield* Effect.tryPromise({
      try: () => importConfigModule(configModuleUrl),
      catch: (error) =>
        invalidStartupEnv(
          `Failed to import VIEW_SERVER_CONFIG_MODULE ${configModuleUrl}`,
          error,
          "VIEW_SERVER_CONFIG_MODULE",
        ),
    });
    const config = yield* Effect.try({
      try: () => readViewServerConfigExport(moduleValue, productionEnv.configExport),
      catch: (error) => invalidConfig("Invalid production config module", "config", error),
    });
    return {
      env: productionEnv,
      config,
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

function toImportUrl(value: string): string {
  if (isAbsoluteUrl(value)) {
    return value;
  }
  return new URL(value, fileUrlForCwd()).href;
}

function isAbsoluteUrl(value: string): boolean {
  try {
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

async function importConfigModule(configModuleUrl: string): Promise<unknown> {
  return import(/* @vite-ignore */ configModuleUrl);
}

function fileUrlForCwd(): string {
  const cwd = typeof process === "undefined" ? "/" : process.cwd();
  return `file://${cwd.endsWith("/") ? cwd : `${cwd}/`}`;
}
