import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { serverShutdown, type ViewServerError } from "../errors.ts";
import type { TopicWorkerHost } from "../worker/index.ts";

export type RuntimeOperation = "query" | "subscribe" | "publish" | "delta-publish" | "delete";

export class RuntimeShutdownController {
  #closing = false;

  isClosing(): boolean {
    return this.#closing;
  }

  ensureOpen(
    operation: RuntimeOperation,
    topic: string,
    requestId?: string,
  ): Effect.Effect<void, ViewServerError> {
    return this.#closing
      ? Effect.fail(
          serverShutdown(`Server is shutting down; refusing ${operation}`, topic, requestId),
        )
      : Effect.void;
  }

  readonly close = (args: {
    readonly syncHealth: Effect.Effect<void>;
    readonly sourceFibers: readonly Fiber.Fiber<void, ViewServerError>[];
    readonly workers: Iterable<TopicWorkerHost>;
  }): Effect.Effect<void, ViewServerError> =>
    Effect.suspend(() => {
      if (this.#closing) {
        return Effect.void;
      }
      this.#closing = true;
      return Effect.fn("view-server.runtime.shutdown")(function* () {
        yield* args.syncHealth;
        yield* Effect.forEach(args.sourceFibers, (fiber) => Fiber.interrupt(fiber), {
          discard: true,
        }).pipe(Effect.ignore);
        yield* Effect.forEach(args.workers, (worker) => worker.shutdown, { discard: true });
      })();
    });
}
