import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type { ViewServerError } from "../errors.ts";

export class RuntimeHealthSyncScheduler {
  readonly #syncNow: Effect.Effect<void, ViewServerError>;
  readonly #scope: Scope.Scope;
  readonly #delayMs: number;
  #scheduled = false;
  #pending = false;
  #closed = false;

  constructor(args: {
    readonly syncNow: Effect.Effect<void, ViewServerError>;
    readonly scope: Scope.Scope;
    readonly delayMs?: number | undefined;
  }) {
    this.#syncNow = args.syncNow;
    this.#scope = args.scope;
    this.#delayMs = Math.max(0, args.delayMs ?? 25);
  }

  readonly request: Effect.Effect<void> = Effect.suspend(() => {
    if (this.#closed) {
      return Effect.void;
    }
    this.#pending = true;
    if (this.#scheduled) {
      return Effect.void;
    }
    this.#scheduled = true;
    return this.#scheduledFlush().pipe(Effect.forkIn(this.#scope), Effect.asVoid);
  });

  readonly flush: Effect.Effect<void, ViewServerError> = Effect.suspend(() => {
    if (this.#closed) {
      return Effect.void;
    }
    this.#pending = false;
    return this.#syncNow;
  });

  readonly close: Effect.Effect<void> = Effect.sync(() => {
    this.#closed = true;
    this.#pending = false;
  });

  #scheduledFlush(): Effect.Effect<void> {
    return Effect.fn("view-server.runtime.health_topic.scheduled_sync")(function* (
      scheduler: RuntimeHealthSyncScheduler,
    ) {
      if (scheduler.#delayMs > 0) {
        yield* Effect.sleep(`${scheduler.#delayMs} millis`);
      }
      while (scheduler.#pending && !scheduler.#closed) {
        scheduler.#pending = false;
        yield* scheduler.#syncNow.pipe(
          Effect.catchCause(() =>
            Effect.logWarning("view-server health topic scheduled sync failed"),
          ),
        );
      }
    })(this).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          this.#scheduled = false;
        }),
      ),
    );
  }
}
