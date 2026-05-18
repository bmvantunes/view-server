import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import type { WebsocketFanoutMetricsSnapshot } from "./websocket-fanout.ts";

export type WebsocketTransportInitialMessage = {
  readonly path: string;
  readonly host: string;
  readonly port: number;
};

export type WebsocketTransportReadyMessage = {
  readonly type: "ready";
  readonly host: string;
  readonly port: number;
  readonly path: string;
};

export type WebsocketTransportClientConnectedMessage = {
  readonly type: "clientConnected";
  readonly clientId: number;
};

export type WebsocketTransportClientDisconnectedMessage = {
  readonly type: "clientDisconnected";
  readonly clientId: number;
};

export type WebsocketTransportClientMessage = {
  readonly type: "clientMessage";
  readonly clientId: number;
  readonly message: FromClientEncoded;
};

export type WebsocketTransportMetricsResponse = {
  readonly type: "metrics";
  readonly requestId: number;
  readonly snapshot: WebsocketFanoutMetricsSnapshot;
};

export type WebsocketTransportShutdownAck = {
  readonly type: "shutdownAck";
  readonly requestId: number;
};

export type WebsocketTransportWorkerMessage =
  | WebsocketTransportReadyMessage
  | WebsocketTransportClientConnectedMessage
  | WebsocketTransportClientDisconnectedMessage
  | WebsocketTransportClientMessage
  | WebsocketTransportMetricsResponse
  | WebsocketTransportShutdownAck;

export type WebsocketTransportServerMessage =
  | {
      readonly type: "serverMessage";
      readonly clientId: number;
      readonly response: FromServerEncoded;
    }
  | {
      readonly type: "serverBatch";
      readonly entries: readonly WebsocketTransportServerBatchEntry[];
    }
  | {
      readonly type: "metrics";
      readonly requestId: number;
    }
  | {
      readonly type: "shutdown";
      readonly requestId: number;
    };

export type WebsocketTransportServerBatchEntry = {
  readonly clientId: number;
  readonly response: FromServerEncoded;
};

export function isWebsocketTransportInitialMessage(
  value: unknown,
): value is WebsocketTransportInitialMessage {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.path === "string" &&
    typeof value.host === "string" &&
    typeof value.port === "number" &&
    Number.isInteger(value.port) &&
    value.port >= 0
  );
}

export function isWebsocketTransportWorkerMessage(
  value: unknown,
): value is WebsocketTransportWorkerMessage {
  if (!isRecord(value) || typeof value.type !== "string") {
    return false;
  }
  switch (value.type) {
    case "ready":
      return (
        typeof value.host === "string" &&
        typeof value.port === "number" &&
        Number.isInteger(value.port) &&
        typeof value.path === "string"
      );
    case "clientConnected":
    case "clientDisconnected":
      return typeof value.clientId === "number" && Number.isInteger(value.clientId);
    case "clientMessage":
      return (
        typeof value.clientId === "number" &&
        Number.isInteger(value.clientId) &&
        isFromClientEncoded(value.message)
      );
    case "metrics":
      return (
        typeof value.requestId === "number" &&
        Number.isInteger(value.requestId) &&
        isWebsocketFanoutMetricsSnapshot(value.snapshot)
      );
    case "shutdownAck":
      return typeof value.requestId === "number" && Number.isInteger(value.requestId);
    default:
      return false;
  }
}

export function isWebsocketTransportServerMessage(
  value: unknown,
): value is WebsocketTransportServerMessage {
  if (!isRecord(value) || typeof value.type !== "string") {
    return false;
  }
  switch (value.type) {
    case "serverMessage":
      return (
        typeof value.clientId === "number" &&
        Number.isInteger(value.clientId) &&
        isFromServerEncoded(value.response)
      );
    case "serverBatch":
      return Array.isArray(value.entries) && value.entries.every(isServerBatchEntry);
    case "metrics":
    case "shutdown":
      return typeof value.requestId === "number" && Number.isInteger(value.requestId);
    default:
      return false;
  }
}

function isServerBatchEntry(value: unknown): value is WebsocketTransportServerBatchEntry {
  return (
    isRecord(value) &&
    typeof value.clientId === "number" &&
    Number.isInteger(value.clientId) &&
    isFromServerEncoded(value.response)
  );
}

export function isFromClientEncoded(value: unknown): value is FromClientEncoded {
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

function isWebsocketFanoutMetricsSnapshot(value: unknown): value is WebsocketFanoutMetricsSnapshot {
  if (!isRecord(value)) {
    return false;
  }
  return (
    (value.transportMode === "in-process" || value.transportMode === "isolated") &&
    numberField(value, "activeClients") &&
    numberField(value, "totalMessages") &&
    numberField(value, "totalBatches") &&
    numberField(value, "totalBytes") &&
    numberField(value, "totalEncodeMs") &&
    numberField(value, "totalWriteMs") &&
    numberField(value, "totalProtocolOfferMs") &&
    numberField(value, "totalProtocolQueueWaitMs") &&
    numberField(value, "maxClientQueuedMessages") &&
    numberField(value, "maxClientQueuedBytes") &&
    numberField(value, "maxBatchMessages") &&
    numberField(value, "maxBatchBytes") &&
    numberField(value, "maxEncodeMs") &&
    numberField(value, "maxWriteMs") &&
    numberField(value, "maxProtocolOfferMs") &&
    numberField(value, "maxProtocolQueueWaitMs") &&
    isTransportEventLoopDelayStats(value.transportEventLoopDelay)
  );
}

function isFromServerEncoded(value: unknown): value is FromServerEncoded {
  if (!isRecord(value)) {
    return false;
  }
  switch (value._tag) {
    case "Chunk":
      return typeof value.requestId === "string" && Array.isArray(value.values);
    case "Exit":
      return typeof value.requestId === "string" && isRecord(value.exit);
    case "Defect":
      return true;
    case "Pong":
      return true;
    case "ClientProtocolError":
      return isRecord(value.error);
    default:
      return false;
  }
}

function isTransportEventLoopDelayStats(value: unknown): boolean {
  return (
    isRecord(value) &&
    numberField(value, "minMs") &&
    numberField(value, "meanMs") &&
    numberField(value, "maxMs") &&
    numberField(value, "stddevMs") &&
    numberField(value, "p50Ms") &&
    numberField(value, "p95Ms") &&
    numberField(value, "p99Ms")
  );
}

function numberField(parent: Readonly<Record<string, unknown>>, field: string): boolean {
  const value = parent[field];
  return typeof value === "number" && Number.isFinite(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
