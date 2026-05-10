import type { Effect } from "effect";
import type * as Scope from "effect/Scope";
import type { TopicConfig } from "../config/index.ts";
import type { ViewServerError } from "../errors.ts";
import type { SnapshotBackend } from "../snapshot/index.ts";
import { makeTopicWorkerCore, type TopicWorkerCore } from "./topic-worker-core.ts";

export type TopicWorkerHost = TopicWorkerCore;

export type TopicWorkerHostOptions = {
  readonly initialRows?: readonly Record<string, unknown>[] | undefined;
  readonly snapshotBackend?: SnapshotBackend | undefined;
  readonly mutationLogSize?: number | undefined;
};

export type TopicWorkerHostFactory = (
  topic: string,
  config: TopicConfig,
  options: TopicWorkerHostOptions,
) => Effect.Effect<TopicWorkerHost, ViewServerError, Scope.Scope>;

export const makeInProcessTopicWorkerHost: TopicWorkerHostFactory = (topic, config, options) =>
  makeTopicWorkerCore(topic, config, options);
