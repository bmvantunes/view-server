import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import { HttpRouter } from "effect/unstable/http";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import { ResponseDefectEncoded } from "effect/unstable/rpc/RpcMessage";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import type * as Socket from "effect/unstable/socket/Socket";
import { Buffer } from "node:buffer";
import { performance } from "node:perf_hooks";

export type WebsocketFanoutMetricsSnapshot = {
  readonly activeClients: number;
  readonly totalMessages: number;
  readonly totalBatches: number;
  readonly totalBytes: number;
  readonly totalEncodeMs: number;
  readonly totalWriteMs: number;
  readonly totalProtocolOfferMs: number;
  readonly totalProtocolQueueWaitMs: number;
  readonly maxClientQueuedMessages: number;
  readonly maxClientQueuedBytes: number;
  readonly maxBatchMessages: number;
  readonly maxBatchBytes: number;
  readonly maxEncodeMs: number;
  readonly maxWriteMs: number;
  readonly maxProtocolOfferMs: number;
  readonly maxProtocolQueueWaitMs: number;
};

type WebsocketFanoutMetricsShape = {
  readonly clientConnected: (clientId: number) => void;
  readonly clientDisconnected: (clientId: number) => void;
  readonly recordQueued: (clientId: number, queuedMessages: number) => void;
  readonly recordProtocolOffer: (offerMs: number) => void;
  readonly recordFlush: (sample: WebsocketFanoutFlushSample) => void;
  readonly snapshot: Effect.Effect<WebsocketFanoutMetricsSnapshot>;
};

type WebsocketFanoutFlushSample = {
  readonly clientId: number;
  readonly messages: number;
  readonly bytes: number;
  readonly encodeMs: number;
  readonly writeMs: number;
  readonly queueWaitMs: number;
};

type WebsocketClientState = {
  readonly queue: Queue.Queue<FromServerEncoded>;
};

export class ViewServerWebsocketFanoutMetrics extends Context.Service<
  ViewServerWebsocketFanoutMetrics,
  WebsocketFanoutMetricsShape
>()("@view-server/core/ViewServerWebsocketFanoutMetrics") {}

export const layerBatchedWebsocketProtocolRoute = (path: HttpRouter.PathInput = "/rpc") =>
  Layer.effectContext(
    Effect.fn("view-server.rpc.websocket.protocol.layer")(function* () {
      const metrics = makeWebsocketFanoutMetrics();
      const protocol = yield* makeBatchedWebsocketProtocol({ path, metrics });
      return Context.make(RpcServer.Protocol, protocol).pipe(
        Context.add(ViewServerWebsocketFanoutMetrics, metrics),
      );
    })(),
  );

const makeBatchedWebsocketProtocol = Effect.fn("view-server.rpc.websocket.protocol.make")(
  function* (options: {
    readonly path: HttpRouter.PathInput;
    readonly metrics: WebsocketFanoutMetricsShape;
  }) {
    const { onSocket, protocol } = yield* makeBatchedSocketProtocol(options.metrics);
    const router = yield* HttpRouter.HttpRouter;
    yield* router.add(
      "GET",
      options.path,
      Effect.fn("view-server.rpc.websocket.protocol.upgrade")(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const socket = yield* Effect.orDie(request.upgrade);
        yield* onSocket(socket, Object.entries(request.headers));
        return HttpServerResponse.empty();
      })(),
    );
    return protocol;
  },
);

function makeBatchedSocketProtocol(metrics: WebsocketFanoutMetricsShape): Effect.Effect<
  {
    readonly protocol: RpcServer.Protocol["Service"];
    readonly onSocket: (
      socket: Socket.Socket,
      headers?: ReadonlyArray<[string, string]>,
    ) => Effect.Effect<void, never, Scope.Scope>;
  },
  never,
  RpcSerialization.RpcSerialization
> {
  return Effect.gen(function* () {
    const serialization = yield* RpcSerialization.RpcSerialization;
    const disconnects = yield* Queue.make<number>();
    let clientId = 0;
    const clients = new Map<number, WebsocketClientState>();
    const clientIds = new Set<number>();
    let writeRequest: (clientId: number, message: FromClientEncoded) => Effect.Effect<void> = () =>
      Effect.void;

    const onSocket = Effect.fn("view-server.rpc.websocket.protocol.socket")(function* (
      socket: Socket.Socket,
      headers?: ReadonlyArray<[string, string]>,
    ) {
      const scope = yield* Effect.scope;
      const parser = serialization.makeUnsafe();
      const id = clientId;
      clientId += 1;
      const queue = yield* Queue.unbounded<FromServerEncoded>();
      const writeRaw = yield* socket.writer;
      const client: WebsocketClientState = { queue };

      clients.set(id, client);
      clientIds.add(id);
      metrics.clientConnected(id);

      yield* flushClientWrites({
        clientId: id,
        queue,
        writeRaw,
        parser,
        metrics,
      }).pipe(Effect.forkIn(scope, { startImmediately: true }));

      yield* Scope.addFinalizerExit(scope, () =>
        Effect.sync(() => {
          clients.delete(id);
          clientIds.delete(id);
          metrics.clientDisconnected(id);
        }).pipe(Effect.flatMap(() => Queue.offer(disconnects, id))),
      );

      yield* socket
        .runRaw((data) =>
          handleSocketData({
            data,
            headers,
            clientId: id,
            parser,
            writeRaw,
            writeRequest,
          }),
        )
        .pipe(
          Effect.catchReason("SocketError", "SocketCloseError", () => Effect.void),
          Effect.orDie,
        );
    });

    const protocol = yield* RpcServer.Protocol.make((writeRequest_) => {
      writeRequest = writeRequest_;
      return Effect.succeed({
        disconnects,
        send: (id, response) => {
          const client = clients.get(id);
          if (client === undefined) {
            return Effect.void;
          }
          const offerStartedAt = performance.now();
          return Queue.offer(client.queue, response).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                metrics.recordProtocolOffer(performance.now() - offerStartedAt);
              }),
            ),
            Effect.flatMap(() => Queue.size(client.queue)),
            Effect.tap((queuedMessages) =>
              Effect.sync(() => {
                metrics.recordQueued(id, queuedMessages);
              }),
            ),
            Effect.asVoid,
          );
        },
        end() {
          return Effect.void;
        },
        clientIds: Effect.sync(() => clientIds),
        initialMessage: Effect.succeedNone,
        supportsAck: true,
        supportsTransferables: false,
        supportsSpanPropagation: true,
      });
    });

    return { protocol, onSocket };
  });
}

function flushClientWrites(args: {
  readonly clientId: number;
  readonly queue: Queue.Queue<FromServerEncoded>;
  readonly writeRaw: (
    chunk: Uint8Array | string | Socket.CloseEvent,
  ) => Effect.Effect<void, Socket.SocketError>;
  readonly parser: RpcSerialization.Parser;
  readonly metrics: WebsocketFanoutMetricsShape;
}): Effect.Effect<void> {
  return Effect.forever(
    Effect.gen(function* () {
      const first = yield* Queue.take(args.queue);
      yield* Effect.yieldNow;
      const rest = yield* Queue.clear(args.queue);
      const batch = [first, ...rest];
      const encodeStartedAt = performance.now();
      const encoded = encodeServerBatch(args.parser, batch);
      const encodeMs = performance.now() - encodeStartedAt;
      const queuedMessages = yield* Queue.size(args.queue);
      yield* Effect.sync(() => {
        args.metrics.recordQueued(args.clientId, queuedMessages);
      });
      if (encoded === undefined) {
        return;
      }
      const bytes = payloadBytes(encoded);
      const queueWaitMs = serverBatchQueueWaitMs(batch, Date.now());
      const writeStartedAt = performance.now();
      yield* args.writeRaw(encoded).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            args.metrics.recordFlush({
              clientId: args.clientId,
              messages: batch.length,
              bytes,
              encodeMs,
              writeMs: performance.now() - writeStartedAt,
              queueWaitMs,
            });
          }),
        ),
        Effect.orDie,
      );
    }),
  );
}

function handleSocketData(args: {
  readonly data: string | Uint8Array;
  readonly headers: ReadonlyArray<[string, string]> | undefined;
  readonly clientId: number;
  readonly parser: RpcSerialization.Parser;
  readonly writeRaw: (
    chunk: Uint8Array | string | Socket.CloseEvent,
  ) => Effect.Effect<void, Socket.SocketError>;
  readonly writeRequest: (clientId: number, message: FromClientEncoded) => Effect.Effect<void>;
}): Effect.Effect<void, Socket.SocketError> {
  return Effect.suspend(() => {
    const decoded = decodeClientMessages(args.parser, args.data);
    if (decoded._tag === "Defect") {
      return writeDefect(args.parser, args.writeRaw, decoded.cause);
    }
    if (decoded.messages.length === 0) {
      return Effect.void;
    }
    let index = 0;
    return Effect.whileLoop({
      while: () => index < decoded.messages.length,
      body: () => {
        const message = decoded.messages[index];
        index += 1;
        if (message === undefined) {
          return Effect.void;
        }
        return args.writeRequest(args.clientId, withRequestHeaders(message, args.headers));
      },
      step: () => undefined,
    });
  });
}

function decodeClientMessages(
  parser: RpcSerialization.Parser,
  data: string | Uint8Array,
):
  | {
      readonly _tag: "Messages";
      readonly messages: readonly FromClientEncoded[];
    }
  | {
      readonly _tag: "Defect";
      readonly cause: unknown;
    } {
  try {
    const decoded = parser.decode(data);
    const messages: FromClientEncoded[] = [];
    for (const message of decoded) {
      if (!isFromClientEncoded(message)) {
        return {
          _tag: "Defect",
          cause: new Error("Invalid RPC websocket client message"),
        };
      }
      messages.push(message);
    }
    return {
      _tag: "Messages",
      messages,
    };
  } catch (cause) {
    return {
      _tag: "Defect",
      cause,
    };
  }
}

function withRequestHeaders(
  message: FromClientEncoded,
  headers: ReadonlyArray<[string, string]> | undefined,
): FromClientEncoded {
  return headers !== undefined && message._tag === "Request"
    ? {
        ...message,
        headers: [...headers, ...message.headers],
      }
    : message;
}

function encodeServerBatch(
  parser: RpcSerialization.Parser,
  batch: readonly FromServerEncoded[],
): Uint8Array | string | undefined {
  try {
    return parser.encode(batch.length === 1 ? batch[0] : batch);
  } catch (cause) {
    return parser.encode(ResponseDefectEncoded(cause));
  }
}

function writeDefect(
  parser: RpcSerialization.Parser,
  writeRaw: (
    chunk: Uint8Array | string | Socket.CloseEvent,
  ) => Effect.Effect<void, Socket.SocketError>,
  cause: unknown,
): Effect.Effect<void, Socket.SocketError> {
  const encoded = parser.encode(ResponseDefectEncoded(cause));
  return encoded === undefined ? Effect.void : writeRaw(encoded);
}

function payloadBytes(payload: Uint8Array | string): number {
  return typeof payload === "string" ? Buffer.byteLength(payload) : payload.byteLength;
}

function isFromClientEncoded(value: unknown): value is FromClientEncoded {
  if (!isRecord(value)) {
    return false;
  }
  switch (value._tag) {
    case "Request":
      return (
        typeof value.id === "string" &&
        typeof value.tag === "string" &&
        Array.isArray(value.headers)
      );
    case "Ack":
    case "Interrupt":
      return typeof value.requestId === "string";
    case "Ping":
    case "Eof":
      return true;
    default:
      return false;
  }
}

function makeWebsocketFanoutMetrics(): WebsocketFanoutMetricsShape {
  const clientQueuedMessages = new Map<number, number>();
  let totalMessages = 0;
  let totalBatches = 0;
  let totalBytes = 0;
  let totalEncodeMs = 0;
  let totalWriteMs = 0;
  let totalProtocolOfferMs = 0;
  let totalProtocolQueueWaitMs = 0;
  let maxClientQueuedMessages = 0;
  let maxClientQueuedBytes = 0;
  let maxBatchMessages = 0;
  let maxBatchBytes = 0;
  let maxEncodeMs = 0;
  let maxWriteMs = 0;
  let maxProtocolOfferMs = 0;
  let maxProtocolQueueWaitMs = 0;

  return {
    clientConnected: (clientId) => {
      clientQueuedMessages.set(clientId, 0);
    },
    clientDisconnected: (clientId) => {
      clientQueuedMessages.delete(clientId);
    },
    recordQueued: (clientId, queuedMessages) => {
      clientQueuedMessages.set(clientId, queuedMessages);
      maxClientQueuedMessages = Math.max(maxClientQueuedMessages, queuedMessages);
    },
    recordProtocolOffer: (offerMs) => {
      totalProtocolOfferMs += offerMs;
      maxProtocolOfferMs = Math.max(maxProtocolOfferMs, offerMs);
    },
    recordFlush: (sample) => {
      totalMessages += sample.messages;
      totalBatches += 1;
      totalBytes += sample.bytes;
      totalEncodeMs += sample.encodeMs;
      totalWriteMs += sample.writeMs;
      totalProtocolQueueWaitMs += sample.queueWaitMs;
      maxClientQueuedBytes = Math.max(maxClientQueuedBytes, sample.bytes);
      maxBatchMessages = Math.max(maxBatchMessages, sample.messages);
      maxBatchBytes = Math.max(maxBatchBytes, sample.bytes);
      maxEncodeMs = Math.max(maxEncodeMs, sample.encodeMs);
      maxWriteMs = Math.max(maxWriteMs, sample.writeMs);
      maxProtocolQueueWaitMs = Math.max(maxProtocolQueueWaitMs, sample.queueWaitMs);
      clientQueuedMessages.set(sample.clientId, 0);
    },
    snapshot: Effect.sync(() => ({
      activeClients: clientQueuedMessages.size,
      totalMessages,
      totalBatches,
      totalBytes,
      totalEncodeMs,
      totalWriteMs,
      totalProtocolOfferMs,
      totalProtocolQueueWaitMs,
      maxClientQueuedMessages,
      maxClientQueuedBytes,
      maxBatchMessages,
      maxBatchBytes,
      maxEncodeMs,
      maxWriteMs,
      maxProtocolOfferMs,
      maxProtocolQueueWaitMs,
    })),
  };
}

function serverBatchQueueWaitMs(batch: readonly FromServerEncoded[], nowMs: number): number {
  let max = 0;
  for (const response of batch) {
    const responseMax = responseQueueWaitMs(response, nowMs);
    if (responseMax > max) {
      max = responseMax;
    }
  }
  return max;
}

function responseQueueWaitMs(response: FromServerEncoded, nowMs: number): number {
  if (response._tag !== "Chunk") {
    return 0;
  }
  let max = 0;
  for (const value of response.values) {
    const serverTime = valueServerTime(value);
    if (serverTime !== undefined) {
      max = Math.max(max, Math.max(0, nowMs - serverTime));
    }
  }
  return max;
}

function valueServerTime(value: unknown): number | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const meta = value.meta;
  if (!isRecord(meta)) {
    return undefined;
  }
  return typeof meta.serverTime === "number" && Number.isFinite(meta.serverTime)
    ? meta.serverTime
    : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
