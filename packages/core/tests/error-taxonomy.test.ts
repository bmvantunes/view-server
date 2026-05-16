import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import {
  BackpressureExceeded,
  ChdbChildExited,
  InvalidConfig,
  QueryLimitExceeded,
  ServerShutdown,
  SnapshotBackendUnavailable,
  SnapshotReplayGap,
  SourceFailed,
  UnauthorizedSystemTopic,
  ViewServerError,
  backpressureExceeded,
  isRetryableViewServerError,
  transportError,
  viewServerErrorRetryAction,
  type ViewServerError as ViewServerErrorType,
} from "../src/errors.ts";

const taxonomySamples: readonly ViewServerErrorType[] = [
  new InvalidConfig({
    field: "worker.maxQueueDepth",
    message: "worker.maxQueueDepth must be positive",
  }),
  new ServerShutdown({
    topic: "orders",
    requestId: "request-1",
    message: "server is shutting down",
  }),
  backpressureExceeded("request-1", "subscription queue is full"),
  new QueryLimitExceeded({
    topic: "orders",
    field: "maxPageSize",
    limit: 50,
    actual: 500,
    message: "page size too large",
  }),
  new UnauthorizedSystemTopic({
    topic: "__private",
    operation: "query",
    message: "system topic is private",
  }),
  new SnapshotBackendUnavailable({
    topic: "orders",
    message: "snapshot backend unavailable",
  }),
  new SnapshotReplayGap({
    topic: "orders",
    backendVersion: "10",
    targetVersion: "50",
    message: "snapshot replay gap",
  }),
  new SourceFailed({
    topic: "orders",
    source: "KafkaSource",
    message: "source failed",
  }),
  new ChdbChildExited({
    topic: "orders",
    pid: 123,
    signal: "SIGTERM",
    message: "chDB child exited",
  }),
];

describe("view server error taxonomy", () => {
  it("serializes and deserializes every explicit public taxonomy error", () => {
    for (const error of taxonomySamples) {
      const encoded = Schema.encodeUnknownSync(ViewServerError)(error);
      const decoded = Schema.decodeUnknownSync(ViewServerError)(encoded);
      expect(decoded._tag).toBe(error._tag);
      expect(decoded.message).toBe(error.message);
    }
  });

  it("keeps subscription retry actions explicit and narrow", () => {
    const transport = transportError("socket closed");
    const backpressure = new BackpressureExceeded({
      requestId: "request-1",
      message: "queue full",
    });

    expect(viewServerErrorRetryAction(transport)).toBe("retry");
    expect(viewServerErrorRetryAction(backpressure)).toBe("resubscribe");
    expect(isRetryableViewServerError(transport)).toBe(true);
    expect(isRetryableViewServerError(backpressure)).toBe(true);

    for (const error of taxonomySamples) {
      if (error._tag !== "BackpressureExceeded") {
        expect(viewServerErrorRetryAction(error)).toBe("fail");
        expect(isRetryableViewServerError(error)).toBe(false);
      }
    }
  });
});
