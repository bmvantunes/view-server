import * as Effect from "effect/Effect";
import { ResponseDefectEncoded } from "effect/unstable/rpc/RpcMessage";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import { Buffer } from "node:buffer";
import { createServer, type IncomingHttpHeaders, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { parentPort, workerData } from "node:worker_threads";
import { WebSocket, WebSocketServer, type RawData } from "ws";
import {
  encodeServerBatch,
  makeWebsocketFanoutMetrics,
  payloadBytes,
  serverBatchQueueWaitMs,
  withRequestHeaders,
} from "./websocket-fanout.ts";
import {
  isFromClientEncoded,
  isWebsocketTransportInitialMessage,
  isWebsocketTransportServerMessage,
  type WebsocketTransportInitialMessage,
  type WebsocketTransportReadyMessage,
  type WebsocketTransportServerBatchEntry,
  type WebsocketTransportServerMessage,
} from "./websocket-transport-protocol.ts";

type ClientState = {
  readonly clientId: number;
  readonly socket: WebSocket;
  readonly queue: import("effect/unstable/rpc/RpcMessage").FromServerEncoded[];
  flushScheduled: boolean;
  flushing: boolean;
};

const parent = parentPort;
const initialMessage: unknown = workerData;

if (parent === null) {
  throw new Error("Isolated websocket transport worker requires parentPort");
}
if (!isWebsocketTransportInitialMessage(initialMessage)) {
  throw new Error("Invalid isolated websocket transport worker initial message");
}

runTransportWorker(parent, initialMessage);

function runTransportWorker(
  port: NonNullable<typeof parentPort>,
  initial: WebsocketTransportInitialMessage,
): void {
  const parser = RpcSerialization.ndjson.makeUnsafe();
  const eventLoopDelay = monitorEventLoopDelay({ resolution: 10 });
  eventLoopDelay.enable();
  const metrics = makeWebsocketFanoutMetrics("isolated", () =>
    transportEventLoopDelayStats(eventLoopDelay),
  );
  const server = createServer();
  const websocketServer = new WebSocketServer({ noServer: true });
  const clients = new Map<number, ClientState>();
  let nextClientId = 0;
  let shutdownStarted = false;

  server.on("upgrade", (request, socket, head) => {
    if (request.url !== initial.path) {
      socket.destroy();
      return;
    }
    websocketServer.handleUpgrade(request, socket, head, (websocket) => {
      websocketServer.emit("connection", websocket, request);
    });
  });

  websocketServer.on("connection", (socket: WebSocket, request: IncomingMessage) => {
    if (shutdownStarted) {
      socket.close(1012, "shutdown");
      return;
    }
    const clientId = nextClientId;
    nextClientId += 1;
    const client: ClientState = {
      clientId,
      socket,
      queue: [],
      flushScheduled: false,
      flushing: false,
    };
    clients.set(clientId, client);
    metrics.clientConnected(clientId);
    port.postMessage({ type: "clientConnected", clientId });
    const headers = requestHeaders(request.headers);

    socket.on("message", (data) => {
      handleClientData({ port, parser, clientId, headers, socket, data });
    });
    socket.on("error", (cause) => {
      logTransportError("isolated websocket client socket error", cause);
    });
    socket.on("close", () => {
      disconnectClient({ port, metrics, clients, clientId });
    });
  });

  server.on("error", (cause) => {
    logTransportError("isolated websocket transport server error", cause);
  });

  port.on("message", (message: unknown) => {
    if (!isWebsocketTransportServerMessage(message)) {
      logTransportError("isolated websocket transport received invalid parent message", message);
      return;
    }
    handleParentMessage({
      message,
      port,
      server,
      websocketServer,
      clients,
      metrics,
      parser,
      eventLoopDelay,
      setShutdownStarted: () => {
        shutdownStarted = true;
      },
    });
  });

  server.listen(initial.port, initial.host, () => {
    const address = server.address();
    const readyPort = tcpPort(address);
    if (readyPort === undefined) {
      logTransportError("isolated websocket transport did not bind a TCP address", address);
      return;
    }
    const ready: WebsocketTransportReadyMessage = {
      type: "ready",
      host: initial.host,
      port: readyPort,
      path: initial.path,
    };
    port.postMessage(ready);
  });
}

function handleParentMessage(args: {
  readonly message: WebsocketTransportServerMessage;
  readonly port: NonNullable<typeof parentPort>;
  readonly server: ReturnType<typeof createServer>;
  readonly websocketServer: WebSocketServer;
  readonly clients: Map<number, ClientState>;
  readonly metrics: ReturnType<typeof makeWebsocketFanoutMetrics>;
  readonly parser: RpcSerialization.Parser;
  readonly eventLoopDelay: ReturnType<typeof monitorEventLoopDelay>;
  readonly setShutdownStarted: () => void;
}): void {
  switch (args.message.type) {
    case "serverMessage": {
      enqueueServerResponse({
        entry: args.message,
        clients: args.clients,
        parser: args.parser,
        metrics: args.metrics,
      });
      return;
    }
    case "serverBatch": {
      for (const entry of args.message.entries) {
        enqueueServerResponse({
          entry,
          clients: args.clients,
          parser: args.parser,
          metrics: args.metrics,
        });
      }
      return;
    }
    case "metrics": {
      const requestId = args.message.requestId;
      Effect.runFork(
        args.metrics.snapshot.pipe(
          Effect.map((snapshot) =>
            args.port.postMessage({
              type: "metrics",
              requestId,
              snapshot,
            }),
          ),
        ),
      );
      return;
    }
    case "shutdown":
      shutdownTransport({
        message: args.message,
        port: args.port,
        server: args.server,
        websocketServer: args.websocketServer,
        clients: args.clients,
        eventLoopDelay: args.eventLoopDelay,
        setShutdownStarted: args.setShutdownStarted,
      });
      return;
  }
}

function enqueueServerResponse(args: {
  readonly entry: WebsocketTransportServerBatchEntry;
  readonly clients: Map<number, ClientState>;
  readonly parser: RpcSerialization.Parser;
  readonly metrics: ReturnType<typeof makeWebsocketFanoutMetrics>;
}): void {
  const client = args.clients.get(args.entry.clientId);
  if (client === undefined || client.socket.readyState !== WebSocket.OPEN) {
    return;
  }
  const offerStartedAt = performance.now();
  client.queue.push(args.entry.response);
  args.metrics.recordProtocolOffer(performance.now() - offerStartedAt);
  args.metrics.recordQueued(client.clientId, client.queue.length);
  scheduleClientFlush(client, args.parser, args.metrics);
}

function scheduleClientFlush(
  client: ClientState,
  parser: RpcSerialization.Parser,
  metrics: ReturnType<typeof makeWebsocketFanoutMetrics>,
): void {
  if (client.flushScheduled || client.flushing) {
    return;
  }
  client.flushScheduled = true;
  setTimeout(() => flushClient(client, parser, metrics), 0);
}

function flushClient(
  client: ClientState,
  parser: RpcSerialization.Parser,
  metrics: ReturnType<typeof makeWebsocketFanoutMetrics>,
): void {
  client.flushScheduled = false;
  if (client.flushing || client.queue.length === 0 || client.socket.readyState !== WebSocket.OPEN) {
    return;
  }
  client.flushing = true;
  const batch = client.queue.splice(0, client.queue.length);
  const encodeStartedAt = performance.now();
  const encoded = encodeServerBatch(parser, batch);
  const encodeMs = performance.now() - encodeStartedAt;
  metrics.recordQueued(client.clientId, client.queue.length);
  if (encoded === undefined) {
    client.flushing = false;
    scheduleClientFlush(client, parser, metrics);
    return;
  }
  const bytes = payloadBytes(encoded);
  const queueWaitMs = serverBatchQueueWaitMs(batch, Date.now());
  const writeStartedAt = performance.now();
  client.socket.send(encoded, (cause) => {
    const writeMs = performance.now() - writeStartedAt;
    metrics.recordFlush({
      clientId: client.clientId,
      messages: batch.length,
      bytes,
      encodeMs,
      writeMs,
      queueWaitMs,
    });
    client.flushing = false;
    if (cause !== undefined && cause !== null) {
      logTransportError("isolated websocket transport write failed", cause);
      client.socket.close(1011, "write failed");
      return;
    }
    scheduleClientFlush(client, parser, metrics);
  });
}

function handleClientData(args: {
  readonly port: NonNullable<typeof parentPort>;
  readonly parser: RpcSerialization.Parser;
  readonly clientId: number;
  readonly headers: ReadonlyArray<[string, string]>;
  readonly socket: WebSocket;
  readonly data: RawData;
}): void {
  const decoded = decodeClientMessages(args.parser, rawDataToParserInput(args.data));
  if (decoded._tag === "Defect") {
    const encoded = args.parser.encode(ResponseDefectEncoded(decoded.cause));
    if (encoded !== undefined && args.socket.readyState === WebSocket.OPEN) {
      args.socket.send(encoded);
    }
    return;
  }
  for (const message of decoded.messages) {
    args.port.postMessage({
      type: "clientMessage",
      clientId: args.clientId,
      message: withRequestHeaders(message, args.headers),
    });
  }
}

function decodeClientMessages(
  parser: RpcSerialization.Parser,
  data: string | Uint8Array,
):
  | {
      readonly _tag: "Messages";
      readonly messages: readonly import("effect/unstable/rpc/RpcMessage").FromClientEncoded[];
    }
  | {
      readonly _tag: "Defect";
      readonly cause: unknown;
    } {
  try {
    const decoded = parser.decode(data);
    const messages: import("effect/unstable/rpc/RpcMessage").FromClientEncoded[] = [];
    for (const message of decoded) {
      if (!isFromClientEncoded(message)) {
        return {
          _tag: "Defect",
          cause: new Error("Invalid isolated RPC websocket client message"),
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

function shutdownTransport(args: {
  readonly message: Extract<WebsocketTransportServerMessage, { readonly type: "shutdown" }>;
  readonly port: NonNullable<typeof parentPort>;
  readonly server: ReturnType<typeof createServer>;
  readonly websocketServer: WebSocketServer;
  readonly clients: Map<number, ClientState>;
  readonly eventLoopDelay: ReturnType<typeof monitorEventLoopDelay>;
  readonly setShutdownStarted: () => void;
}): void {
  args.setShutdownStarted();
  for (const client of args.clients.values()) {
    if (client.socket.readyState === WebSocket.OPEN) {
      client.socket.close(1001, "shutdown");
    } else {
      client.socket.terminate();
    }
  }
  args.websocketServer.close(() => undefined);
  args.server.close(() => {
    args.eventLoopDelay.disable();
    args.port.postMessage({
      type: "shutdownAck",
      requestId: args.message.requestId,
    });
  });
}

function disconnectClient(args: {
  readonly port: NonNullable<typeof parentPort>;
  readonly metrics: ReturnType<typeof makeWebsocketFanoutMetrics>;
  readonly clients: Map<number, ClientState>;
  readonly clientId: number;
}): void {
  if (!args.clients.delete(args.clientId)) {
    return;
  }
  args.metrics.clientDisconnected(args.clientId);
  args.port.postMessage({
    type: "clientDisconnected",
    clientId: args.clientId,
  });
}

function requestHeaders(headers: IncomingHttpHeaders): ReadonlyArray<[string, string]> {
  const entries: [string, string][] = [];
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === "string") {
      entries.push([key, value]);
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        entries.push([key, item]);
      }
    }
  }
  return entries;
}

function rawDataToParserInput(data: RawData): Uint8Array {
  return Array.isArray(data)
    ? Buffer.concat(data)
    : data instanceof ArrayBuffer
      ? new Uint8Array(data)
      : data;
}

function tcpPort(address: string | AddressInfo | null): number | undefined {
  return typeof address === "object" && address !== null ? address.port : undefined;
}

function transportEventLoopDelayStats(
  histogram: ReturnType<typeof monitorEventLoopDelay>,
): import("./websocket-fanout.ts").WebsocketTransportEventLoopDelayStats {
  return {
    minMs: histogramMs(histogram.min),
    meanMs: histogramMs(histogram.mean),
    maxMs: histogramMs(histogram.max),
    stddevMs: histogramMs(histogram.stddev),
    p50Ms: histogramMs(histogram.percentile(50)),
    p95Ms: histogramMs(histogram.percentile(95)),
    p99Ms: histogramMs(histogram.percentile(99)),
  };
}

function histogramMs(value: number): number {
  return Number.isFinite(value) && value >= 0 && value < 1_000_000_000_000 ? value / 1_000_000 : 0;
}

function logTransportError(message: string, cause: unknown): void {
  Effect.runFork(Effect.logError(`${message}: ${String(cause)}`));
}
