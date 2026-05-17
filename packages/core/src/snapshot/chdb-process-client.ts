import { fork, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type {
  ChdbQueryWorkerRequest,
  ChdbQueryWorkerResponse,
  ChdbQueryWorkerSuccessResponse,
} from "./chdb-worker-protocol.ts";
import { isChdbWorkerResponse } from "./chdb-worker-protocol.ts";
import type { SnapshotBackendHealth } from "./snapshot-backend.ts";

export type ChdbProcessClientOptions = {
  readonly workerEntryUrl?: string | URL | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerSpawn?: ((pid: number | undefined) => void) | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerExit?:
    | ((event: { readonly pid: number | undefined; readonly message: string }) => void)
    | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerRequest?:
    | ((request: {
        readonly pid: number | undefined;
        readonly type: ChdbQueryWorkerRequest["type"];
      }) => void)
    | undefined;
};

type PendingRequest = {
  readonly resolve: (response: ChdbQueryWorkerResponse) => void;
  readonly reject: (error: unknown) => void;
};

export class ChdbProcessClient {
  readonly #options: ChdbProcessClientOptions;
  #worker: ChildProcess;
  #topic = "chdb";
  #nextId = 0;
  #pending = new Map<number, PendingRequest>();
  #closed = false;
  #exitMessage: string | undefined;
  #restarts = 0;

  constructor(options: ChdbProcessClientOptions = {}) {
    this.#options = options;
    this.#worker = this.#spawnWorker();
  }

  get pid(): number | undefined {
    return this.#worker.pid;
  }

  get pendingRequests(): number {
    return this.#pending.size;
  }

  get exitedUnexpectedly(): boolean {
    return this.#exitMessage !== undefined;
  }

  get health(): SnapshotBackendHealth {
    if (this.#closed) {
      return {
        status: "stopped",
        pid: this.#worker.pid ?? 0,
        restarts: this.#restarts,
        pendingRequests: this.#pending.size,
        lastError: this.#exitMessage ?? "",
      };
    }
    return this.#exitMessage === undefined
      ? {
          status: "ready",
          pid: this.#worker.pid ?? 0,
          restarts: this.#restarts,
          pendingRequests: this.#pending.size,
          lastError: "",
        }
      : {
          status: "degraded",
          message: this.#exitMessage,
          pid: this.#worker.pid ?? 0,
          restarts: this.#restarts,
          pendingRequests: this.#pending.size,
          lastError: this.#exitMessage,
        };
  }

  setTopic(topic: string): void {
    this.#topic = topic;
  }

  nextRequestId(): number {
    const id = this.#nextId;
    this.#nextId++;
    return id;
  }

  request(
    request: ChdbQueryWorkerRequest,
  ): Effect.Effect<ChdbQueryWorkerSuccessResponse, ViewServerError> {
    return Effect.tryPromise({
      try: () =>
        new Promise<ChdbQueryWorkerResponse>((resolve, reject) => {
          if (this.#exitMessage !== undefined) {
            reject(new Error(this.#exitMessage));
            return;
          }
          if (this.#worker.exitCode !== null || this.#worker.killed || !this.#worker.connected) {
            reject(new Error("chDB worker IPC channel is closed"));
            return;
          }
          this.#pending.set(request.id, { resolve, reject });
          let sent = false;
          try {
            sent = this.#worker.send(request, (error) => {
              if (error === null) {
                return;
              }
              const pending = this.#pending.get(request.id);
              if (pending === undefined) {
                return;
              }
              this.#pending.delete(request.id);
              pending.reject(error);
            });
          } catch (error) {
            this.#pending.delete(request.id);
            reject(error);
            return;
          }
          if (!sent) {
            this.#pending.delete(request.id);
            reject(new Error("chDB worker IPC channel is not writable"));
            return;
          }
          this.#options.onWorkerRequest?.({ pid: this.#worker.pid, type: request.type });
        }).then((response) => {
          if (response.success) {
            return response;
          }
          throw new Error(response.error);
        }),
      catch: (error) => snapshotBackendFailed(this.#topic, error),
    });
  }

  requestClose(deadlineMs: number): Effect.Effect<void, ViewServerError> {
    return Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve) => {
          const id = this.nextRequestId();
          let settled = false;
          let timer: ReturnType<typeof setTimeout> | undefined;
          const settle = () => {
            if (settled) {
              return;
            }
            settled = true;
            if (timer !== undefined) {
              clearTimeout(timer);
            }
            this.#pending.delete(id);
            resolve();
          };
          timer = setTimeout(settle, deadlineMs);
          this.#pending.set(id, {
            resolve: settle,
            reject: settle,
          });
          try {
            const sent = this.#worker.send({ id, type: "close" });
            if (!sent) {
              settle();
            }
          } catch {
            settle();
          }
        }),
      catch: (error) => snapshotBackendFailed(this.#topic, error),
    });
  }

  restart(): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.process.restart")(function* (client: ChdbProcessClient) {
      client.#closed = true;
      yield* client.#terminate().pipe(Effect.ignore);
      client.#restarts++;
      client.#closed = false;
      client.#exitMessage = undefined;
      client.#worker = client.#spawnWorker();
    })(this);
  }

  shutdown(): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.process.shutdown")(function* (client: ChdbProcessClient) {
      if (client.#closed) {
        yield* client.#terminate();
        return;
      }
      client.#closed = true;
      if (client.#worker.exitCode !== null || client.#worker.killed || !client.#worker.connected) {
        yield* client.#terminate();
        return;
      }
      yield* client.requestClose(250).pipe(Effect.flatMap(() => client.#terminate()));
    })(this);
  }

  kill(signal: NodeJS.Signals = "SIGTERM"): void {
    this.#worker.kill(signal);
  }

  #spawnWorker(): ChildProcess {
    const entryUrl = resolveChdbQueryWorkerEntryUrl(this.#options.workerEntryUrl);
    const worker = fork(fileURLToPath(entryUrl), [], {
      execArgv: defaultExecArgv(entryUrl),
      serialization: "advanced",
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    this.#options.onWorkerSpawn?.(worker.pid);
    worker.on("message", (response: unknown) => {
      if (isChdbWorkerResponse(response)) {
        this.#handleResponse(response);
        return;
      }
      this.#failPending(new Error("chDB worker returned an invalid response"));
    });
    worker.on("error", (error) => {
      this.#failPending(error);
    });
    worker.on("exit", (code, signal) => {
      const message =
        signal === null
          ? `chDB worker exited with code ${String(code)}`
          : `chDB worker exited from signal ${signal}`;
      if (!this.#closed && worker === this.#worker) {
        this.#exitMessage = message;
        this.#options.onWorkerExit?.({ pid: worker.pid, message });
      }
      this.#failPending(new Error(message));
    });
    return worker;
  }

  #handleResponse(response: ChdbQueryWorkerResponse): void {
    const pending = this.#pending.get(response.id);
    if (pending === undefined) {
      return;
    }
    this.#pending.delete(response.id);
    pending.resolve(response);
  }

  #failPending(error: unknown): void {
    for (const [id, pending] of this.#pending) {
      this.#pending.delete(id);
      pending.reject(error);
    }
  }

  #terminate(): Effect.Effect<void, ViewServerError> {
    return Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve) => {
          if (this.#worker.exitCode !== null || this.#worker.killed) {
            resolve();
            return;
          }
          let settled = false;
          let forceKillTimer: ReturnType<typeof setTimeout> | undefined;
          let giveUpTimer: ReturnType<typeof setTimeout> | undefined;
          const settle = () => {
            if (settled) {
              return;
            }
            settled = true;
            if (forceKillTimer !== undefined) {
              clearTimeout(forceKillTimer);
            }
            if (giveUpTimer !== undefined) {
              clearTimeout(giveUpTimer);
            }
            resolve();
          };
          this.#worker.once("exit", () => {
            settle();
          });
          forceKillTimer = setTimeout(() => {
            this.#worker.kill("SIGKILL");
          }, 250);
          giveUpTimer = setTimeout(settle, 2_000);
          this.#worker.kill("SIGTERM");
        }),
      catch: (error) => snapshotBackendFailed(this.#topic, error),
    });
  }
}

function resolveChdbQueryWorkerEntryUrl(workerEntryUrl: string | URL | undefined): URL {
  if (workerEntryUrl !== undefined) {
    return toWorkerUrl(workerEntryUrl);
  }
  return import.meta.url.endsWith(".ts")
    ? new URL("./chdb-query-worker-entry.ts", import.meta.url)
    : new URL("./chdb-query-worker-entry.mjs", import.meta.url);
}

function toWorkerUrl(value: string | URL): URL {
  return value instanceof URL ? value : new URL(value);
}

function defaultExecArgv(workerEntryUrl: URL): string[] {
  return workerEntryUrl.pathname.endsWith(".ts") ? ["--experimental-strip-types"] : [];
}
