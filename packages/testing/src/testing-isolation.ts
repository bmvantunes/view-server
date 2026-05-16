import * as Effect from "effect/Effect";
import {
  createViewServerClient,
  type ViewServerClient,
  type ViewServerRpcTransport,
} from "@view-server/core/client";
import type {
  TopicName,
  TopicPatchFromConfig,
  TopicRowFromConfig,
  ViewServerConfig,
} from "@view-server/core/config";
import { VIEW_SERVER_HEALTH_TOPIC } from "@view-server/core/config";
import { isViewServerError, transportError, type ViewServerError } from "@view-server/core/errors";
import type { RuntimeRow } from "@view-server/core/query";
import { toWireRow, type RpcQueryPayload } from "@view-server/core/rpc";

export type TopicRowWithoutIsolation<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = Omit<TopicRowFromConfig<TConfig, TTopic>, "isolationId">;

export type TopicPatchWithoutIsolation<
  TConfig extends ViewServerConfig,
  TTopic extends TopicName<TConfig>,
> = Omit<TopicPatchFromConfig<TConfig, TTopic>, "isolationId">;

export type MissingIsolationTopics<TConfig extends ViewServerConfig> = {
  readonly [TTopic in TopicName<TConfig>]: TopicRowFromConfig<TConfig, TTopic> extends {
    readonly isolationId: string;
  }
    ? never
    : TTopic;
}[TopicName<TConfig>];

export type RequireIsolationId<TConfig extends ViewServerConfig> =
  MissingIsolationTopics<TConfig> extends never
    ? unknown
    : {
        readonly "Each test-isolated topic schema must include isolationId": MissingIsolationTopics<TConfig>;
      };

export type TestingViewServerClient<TConfig extends ViewServerConfig> = {
  readonly query: ViewServerClient<TConfig>["query"];
  readonly subscribe: ViewServerClient<TConfig>["subscribe"];
  readonly health: ViewServerClient<TConfig>["health"];
  readonly createStore: ViewServerClient<TConfig>["createStore"];
  readonly deleteById: ViewServerClient<TConfig>["deleteById"];
  readonly publish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    rows:
      | TopicRowWithoutIsolation<TConfig, TTopic>
      | readonly TopicRowWithoutIsolation<TConfig, TTopic>[],
  ) => Effect.Effect<void, ViewServerError>;
  readonly deltaPublish: <TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchWithoutIsolation<TConfig, TTopic>,
  ) => Effect.Effect<void, ViewServerError>;
};

export function validateTestingIsolationId(isolationId: string): string {
  if (isolationId.trim().length === 0) {
    throw new Error("Testing isolationId is required");
  }
  return isolationId;
}

export function normalizeTestingInitialRows<TConfig extends ViewServerConfig>(
  initialRows:
    | Partial<{
        readonly [TTopic in TopicName<TConfig>]: readonly TopicRowWithoutIsolation<
          TConfig,
          TTopic
        >[];
      }>
    | undefined,
  isolationId: string,
): Readonly<Record<string, readonly RuntimeRow[]>> | undefined {
  if (initialRows === undefined) {
    return undefined;
  }
  const validIsolationId = validateTestingIsolationId(isolationId);
  const normalized: Record<string, readonly RuntimeRow[]> = {};
  for (const [topic, rows] of Object.entries(initialRows)) {
    if (rows !== undefined) {
      normalized[topic] = rows.map((row) => ({ ...row, isolationId: validIsolationId }));
    }
  }
  return normalized;
}

export function createTestingViewServerClientFromTransport<TConfig extends ViewServerConfig>(
  transport: ViewServerRpcTransport,
  config: TConfig,
  isolationId: string,
): {
  readonly client: TestingViewServerClient<TConfig>;
  readonly liveClient: ViewServerClient<TConfig>;
} {
  const validIsolationId = validateTestingIsolationId(isolationId);
  const isolatedTransport = isolateTestingTransport(transport, validIsolationId);
  const liveClient = createViewServerClient<TConfig>(isolatedTransport, config);
  const client: TestingViewServerClient<TConfig> = {
    query: liveClient.query,
    subscribe: liveClient.subscribe,
    health: liveClient.health,
    createStore: liveClient.createStore,
    deleteById: liveClient.deleteById,
    publish: (topic, rows) =>
      Effect.forEach(Array.isArray(rows) ? rows : [rows], (row) =>
        transport.Publish({
          topic,
          row: toWireRow({ ...row, isolationId: validIsolationId }),
        }),
      ).pipe(Effect.asVoid, Effect.mapError(toViewServerError)),
    deltaPublish: (topic, patch) =>
      transport
        .DeltaPublish({
          topic,
          patch: toWireRow({ ...patch, isolationId: validIsolationId }),
        })
        .pipe(Effect.mapError(toViewServerError)),
  };
  return { client, liveClient };
}

export function isolateTestingTransport(
  transport: ViewServerRpcTransport,
  isolationId: string,
): ViewServerRpcTransport {
  const validIsolationId = validateTestingIsolationId(isolationId);
  return {
    Query: (payload) =>
      transport.Query({
        ...payload,
        query: isolateTestingQuery(payload.topic, payload.query, validIsolationId),
      }),
    Subscribe: (payload) =>
      transport.Subscribe({
        ...payload,
        query: isolateTestingQuery(payload.topic, payload.query, validIsolationId),
      }),
    Unsubscribe: transport.Unsubscribe,
    Publish: (payload) =>
      transport.Publish({
        ...payload,
        row: { ...payload.row, isolationId: validIsolationId },
      }),
    DeltaPublish: (payload) =>
      transport.DeltaPublish({
        ...payload,
        patch: { ...payload.patch, isolationId: validIsolationId },
      }),
    DeleteById: transport.DeleteById,
    Health: transport.Health,
  };
}

export function isolateTestingQuery(
  topic: string,
  query: RpcQueryPayload["query"],
  isolationId: string,
): RpcQueryPayload["query"] {
  if (topic === VIEW_SERVER_HEALTH_TOPIC) {
    return query;
  }
  const validIsolationId = validateTestingIsolationId(isolationId);
  const isolationFilter = {
    field: "isolationId",
    comparator: "equals",
    value: validIsolationId,
  } satisfies RpcQueryPayload["query"]["where"];
  const where =
    query.where === undefined
      ? isolationFilter
      : ({
          op: "and",
          conditions: [isolationFilter, query.where],
        } satisfies RpcQueryPayload["query"]["where"]);
  return {
    ...query,
    where,
  };
}

function toViewServerError(error: unknown): ViewServerError {
  return isViewServerError(error) ? error : transportError(error);
}
