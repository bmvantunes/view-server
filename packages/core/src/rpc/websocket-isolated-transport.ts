import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import type { FromClientEncoded } from "effect/unstable/rpc/RpcMessage";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Worker as NodeThreadWorker, type WorkerOptions } from "node:worker_threads";
import {
  emptyTransportEventLoopDelayStats,
  type WebsocketFanoutMetricsShape,
  type WebsocketFanoutMetricsSnapshot,
  ViewServerWebsocketFanoutMetrics,
} from "./websocket-fanout.ts";
import {
  isWebsocketTransportWorkerMessage,
  type WebsocketTransportInitialMessage,
  type WebsocketTransportReadyMessage,
  type WebsocketTransportServerBatchEntry,
  type WebsocketTransportServerMessage,
} from "./websocket-transport-protocol.ts";

export type IsolatedWebsocketTransportOptions = {
  readonly path?: string | undefined;
  readonly host?: string | undefined;
  readonly port?: number | undefined;
  readonly workerEntryUrl?: string | URL | undefined;
  readonly workerName?: string | undefined;
  readonly workerOptions?: Omit<WorkerOptions, "name" | "execArgv" | "workerData"> | undefined;
  readonly execArgv?: readonly string[] | undefined;
  readonly shutdownTimeoutMs?: number | undefined;
  readonly metricsTimeoutMs?: number | undefined;
};

export type ViewServerIsolatedWebsocketTransportAddress = {
  readonly url: string;
  readonly host: string;
  readonly port: number;
  readonly path: string;
};

export class ViewServerIsolatedWebsocketTransport extends Context.Service<
  ViewServerIsolatedWebsocketTransport,
  ViewServerIsolatedWebsocketTransportAddress
>()("@view-server/core/ViewServerIsolatedWebsocketTransport") {}

type PendingMetricRequest = {
  readonly resolve: (snapshot: WebsocketFanoutMetricsSnapshot) => void;
  readonly timeout: ReturnType<typeof setTimeout>;
};

type PendingShutdownRequest = {
  readonly resolve: () => void;
  readonly timeout: ReturnType<typeof setTimeout>;
};

export const layerIsolatedWebsocketProtocol = (options: IsolatedWebsocketTransportOptions = {}) =>
  Layer.effectContext(
    Effect.fn("view-server.rpc.websocket.isolated_transport.layer")(function* () {
      const transport = yield* makeIsolatedWebsocketProtocol(options);
      return Context.make(RpcServer.Protocol, transport.protocol).pipe(
        Context.add(ViewServerWebsocketFanoutMetrics, transport.metrics),
        Context.add(ViewServerIsolatedWebsocketTransport, transport.address),
      );
    })(),
  );

function makeIsolatedWebsocketProtocol(options: IsolatedWebsocketTransportOptions): Effect.Effect<
  {
    readonly protocol: RpcServer.Protocol["Service"];
    readonly metrics: WebsocketFanoutMetricsShape;
    readonly address: ViewServerIsolatedWebsocketTransportAddress;
  },
  never,
  Scope.Scope
> {
  return Effect.gen(function* () {
    const workerEntryUrl = resolveWorkerEntryUrl(options.workerEntryUrl);
    const path = options.path ?? "/rpc";
    const host = options.host ?? "127.0.0.1";
    const initialMessage: WebsocketTransportInitialMessage = {
      path,
      host,
      port: options.port ?? 0,
    };
    const worker = new NodeThreadWorker(workerEntryUrl, {
      ...options.workerOptions,
      name: options.workerName ?? "view-server-websocket-transport",
      execArgv: [...(options.execArgv ?? defaultExecArgv(workerEntryUrl) ?? [])],
      workerData: initialMessage,
    });
    const ready = yield* waitForReady(worker);
    const address: ViewServerIsolatedWebsocketTransportAddress = {
      host: ready.host,
      port: ready.port,
      path: ready.path,
      url: `ws://${ready.host}:${ready.port}${ready.path}`,
    };
    const bridge = yield* makeTransportBridge(worker, options);
    yield* Effect.addFinalizer(() =>
      bridge.shutdown.pipe(
        Effect.andThen(Effect.promise(() => worker.terminate())),
        Effect.catchCause((cause) =>
          Effect.logError(`isolated websocket transport shutdown failed: ${String(cause)}`),
        ),
      ),
    );
    return {
      protocol: bridge.protocol,
      metrics: bridge.metrics,
      address,
    };
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.die(new Error(`Failed to start isolated websocket transport: ${String(cause)}`)),
    ),
  );
}

function makeTransportBridge(
  worker: NodeThreadWorker,
  options: IsolatedWebsocketTransportOptions,
): Effect.Effect<{
  readonly protocol: RpcServer.Protocol["Service"];
  readonly metrics: WebsocketFanoutMetricsShape;
  readonly shutdown: Effect.Effect<void>;
}> {
  return Effect.gen(function* () {
    const disconnects = yield* Queue.make<number>();
    const clientIds = new Set<number>();
    let writeRequest: (clientId: number, message: FromClientEncoded) => Effect.Effect<void> = () =>
      Effect.void;
    let requestId = 0;
    const pendingMetrics = new Map<number, PendingMetricRequest>();
    const pendingShutdown = new Map<number, PendingShutdownRequest>();
    const pendingServerMessages: WebsocketTransportServerBatchEntry[] = [];
    let serverMessageFlushScheduled = false;
    let shuttingDown = false;

    worker.on("message", (message: unknown) => {
      if (!isWebsocketTransportWorkerMessage(message)) {
        Effect.runFork(
          Effect.logError(`isolated websocket transport sent invalid message: ${String(message)}`),
        );
        return;
      }
      switch (message.type) {
        case "ready":
          return;
        case "clientConnected":
          clientIds.add(message.clientId);
          return;
        case "clientDisconnected":
          clientIds.delete(message.clientId);
          Queue.offerUnsafe(disconnects, message.clientId);
          return;
        case "clientMessage":
          Effect.runFork(writeRequest(message.clientId, message.message));
          return;
        case "metrics": {
          const pending = pendingMetrics.get(message.requestId);
          if (pending === undefined) {
            return;
          }
          pendingMetrics.delete(message.requestId);
          clearTimeout(pending.timeout);
          pending.resolve(message.snapshot);
          return;
        }
        case "shutdownAck": {
          const pending = pendingShutdown.get(message.requestId);
          if (pending === undefined) {
            return;
          }
          pendingShutdown.delete(message.requestId);
          clearTimeout(pending.timeout);
          pending.resolve();
          return;
        }
      }
    });

    worker.on("error", (cause) => {
      Effect.runFork(
        Effect.logError(`isolated websocket transport worker error: ${String(cause)}`),
      );
      failPendingMetrics(pendingMetrics);
      failPendingShutdown(pendingShutdown);
    });
    worker.on("exit", (code) => {
      if (code !== 0 && !shuttingDown) {
        Effect.runFork(Effect.logError(`isolated websocket transport exited code=${code}`));
      }
      failPendingMetrics(pendingMetrics);
      failPendingShutdown(pendingShutdown);
    });

    const metrics: WebsocketFanoutMetricsShape = {
      clientConnected: () => undefined,
      clientDisconnected: () => undefined,
      recordQueued: () => undefined,
      recordProtocolOffer: () => undefined,
      recordFlush: () => undefined,
      snapshot: requestMetrics({
        worker,
        pendingMetrics,
        nextRequestId: () => {
          requestId += 1;
          return requestId;
        },
        timeoutMs: options.metricsTimeoutMs ?? 1_000,
      }),
    };

    const protocol: RpcServer.Protocol["Service"] = {
      run: (writeRequest_) =>
        Effect.sync(() => {
          writeRequest = writeRequest_;
        }).pipe(Effect.andThen(Effect.never)),
      disconnects,
      send: (clientId, response) =>
        Effect.sync(() => {
          pendingServerMessages.push({ clientId, response });
          if (!serverMessageFlushScheduled) {
            serverMessageFlushScheduled = true;
            setTimeout(() => {
              serverMessageFlushScheduled = false;
              flushServerMessages(worker, pendingServerMessages);
            }, 0);
          }
        }),
      end: (clientId) =>
        Effect.sync(() => {
          clientIds.delete(clientId);
        }),
      clientIds: Effect.sync(() => clientIds),
      initialMessage: Effect.succeedNone,
      supportsAck: true,
      supportsTransferables: false,
      supportsSpanPropagation: true,
    };

    return {
      protocol,
      metrics,
      shutdown: requestShutdown({
        worker,
        pendingShutdown,
        nextRequestId: () => {
          requestId += 1;
          return requestId;
        },
        timeoutMs: options.shutdownTimeoutMs ?? 1_000,
        onShutdownStart: () => {
          shuttingDown = true;
        },
      }),
    };
  });
}

function flushServerMessages(
  worker: NodeThreadWorker,
  pendingServerMessages: WebsocketTransportServerBatchEntry[],
): void {
  if (pendingServerMessages.length === 0) {
    return;
  }
  const entries = pendingServerMessages.splice(0, pendingServerMessages.length);
  const message: WebsocketTransportServerMessage = {
    type: "serverBatch",
    entries,
  };
  worker.postMessage(message);
}

function waitForReady(worker: NodeThreadWorker): Effect.Effect<WebsocketTransportReadyMessage> {
  return Effect.callback<WebsocketTransportReadyMessage>((resume) => {
    const cleanup = () => {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    };
    const onMessage = (message: unknown) => {
      if (!isWebsocketTransportWorkerMessage(message) || message.type !== "ready") {
        return;
      }
      cleanup();
      resume(Effect.succeed(message));
    };
    const onError = (cause: Error) => {
      cleanup();
      resume(Effect.die(cause));
    };
    const onExit = (code: number) => {
      cleanup();
      resume(Effect.die(new Error(`Isolated websocket transport exited before ready: ${code}`)));
    };
    worker.on("message", onMessage);
    worker.on("error", onError);
    worker.on("exit", onExit);
    return Effect.sync(cleanup);
  });
}

function requestMetrics(args: {
  readonly worker: NodeThreadWorker;
  readonly pendingMetrics: Map<number, PendingMetricRequest>;
  readonly nextRequestId: () => number;
  readonly timeoutMs: number;
}): Effect.Effect<WebsocketFanoutMetricsSnapshot> {
  return Effect.callback<WebsocketFanoutMetricsSnapshot>((resume) => {
    const requestId = args.nextRequestId();
    const timeout = setTimeout(() => {
      args.pendingMetrics.delete(requestId);
      resume(Effect.succeed(emptyIsolatedMetrics()));
    }, args.timeoutMs);
    args.pendingMetrics.set(requestId, {
      resolve: (snapshot) => {
        resume(Effect.succeed(snapshot));
      },
      timeout,
    });
    const message: WebsocketTransportServerMessage = {
      type: "metrics",
      requestId,
    };
    args.worker.postMessage(message);
    return Effect.sync(() => {
      args.pendingMetrics.delete(requestId);
      clearTimeout(timeout);
    });
  });
}

function requestShutdown(args: {
  readonly worker: NodeThreadWorker;
  readonly pendingShutdown: Map<number, PendingShutdownRequest>;
  readonly nextRequestId: () => number;
  readonly timeoutMs: number;
  readonly onShutdownStart: () => void;
}): Effect.Effect<void> {
  return Effect.callback<void>((resume) => {
    const requestId = args.nextRequestId();
    args.onShutdownStart();
    const timeout = setTimeout(() => {
      args.pendingShutdown.delete(requestId);
      resume(Effect.void);
    }, args.timeoutMs);
    args.pendingShutdown.set(requestId, {
      resolve: () => {
        resume(Effect.void);
      },
      timeout,
    });
    const message: WebsocketTransportServerMessage = {
      type: "shutdown",
      requestId,
    };
    args.worker.postMessage(message);
    return Effect.sync(() => {
      args.pendingShutdown.delete(requestId);
      clearTimeout(timeout);
    });
  });
}

function failPendingMetrics(pending: Map<number, PendingMetricRequest>): void {
  for (const [requestId, value] of pending) {
    pending.delete(requestId);
    clearTimeout(value.timeout);
    value.resolve(emptyIsolatedMetrics());
  }
}

function failPendingShutdown(pending: Map<number, PendingShutdownRequest>): void {
  for (const [requestId, value] of pending) {
    pending.delete(requestId);
    clearTimeout(value.timeout);
    value.resolve();
  }
}

function emptyIsolatedMetrics(): WebsocketFanoutMetricsSnapshot {
  return {
    transportMode: "isolated",
    activeClients: 0,
    totalMessages: 0,
    totalBatches: 0,
    totalBytes: 0,
    totalEncodeMs: 0,
    totalWriteMs: 0,
    totalProtocolOfferMs: 0,
    totalProtocolQueueWaitMs: 0,
    maxClientQueuedMessages: 0,
    maxClientQueuedBytes: 0,
    maxBatchMessages: 0,
    maxBatchBytes: 0,
    maxEncodeMs: 0,
    maxWriteMs: 0,
    maxProtocolOfferMs: 0,
    maxProtocolQueueWaitMs: 0,
    transportEventLoopDelay: emptyTransportEventLoopDelayStats(),
  };
}

function resolveWorkerEntryUrl(workerEntryUrl: string | URL | undefined): string | URL {
  if (workerEntryUrl !== undefined) {
    return toWorkerSpecifier(workerEntryUrl);
  }
  return import.meta.url.endsWith(".ts")
    ? new URL("./websocket-transport-worker-entry.ts", import.meta.url)
    : new URL("./rpc/websocket-transport-worker-entry.mjs", import.meta.url);
}

function toWorkerSpecifier(value: string | URL): string | URL {
  if (value instanceof URL) {
    return value;
  }
  if (hasUrlScheme(value)) {
    return new URL(value);
  }
  if (value.endsWith(".ts") || value.endsWith(".mjs") || value.endsWith(".js")) {
    return pathToFileURL(isAbsolute(value) ? value : resolve(value));
  }
  return value;
}

function defaultExecArgv(workerEntryUrl: string | URL): readonly string[] | undefined {
  const value = workerEntryUrl instanceof URL ? workerEntryUrl.pathname : workerEntryUrl;
  return value.endsWith(".ts") ? ["--experimental-strip-types"] : undefined;
}

function hasUrlScheme(value: string): boolean {
  return /^[a-z][a-z\d+.-]*:/i.test(value);
}
