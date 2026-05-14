import { Effect, Exit, Scope } from "effect";
import { useEffect, useMemo, useState } from "react";
import {
  ViewServerMetricsDashboard,
  createViewServerHooks,
  makeBrowserWebsocketClient,
  type ViewServerMetricsHooks,
} from "@view-server/react";
import {
  metricsViewServerConfig,
  resolveViewServerRpcUrl,
  type MetricsViewServerConfig,
} from "../view-server";

type MetricsConnection =
  | { readonly status: "connecting" }
  | {
      readonly status: "ready";
      readonly hooks: ViewServerMetricsHooks;
    }
  | {
      readonly status: "error";
      readonly message: string;
    };

export function MetricsApp(props: { readonly rpcUrl?: string | undefined }) {
  const rpcUrl = useMemo(() => props.rpcUrl ?? resolveViewServerRpcUrl(), [props.rpcUrl]);
  const [connection, setConnection] = useState<MetricsConnection>({ status: "connecting" });

  useEffect(() => {
    let disposed = false;
    let scope: Scope.Closeable | undefined;
    setConnection({ status: "connecting" });

    Effect.runPromise(
      Effect.gen(function* () {
        scope = yield* Scope.make();
        const client = yield* Scope.provide(scope)(
          makeBrowserWebsocketClient<MetricsViewServerConfig>(rpcUrl, metricsViewServerConfig),
        );
        const hooks = createViewServerHooks(client, metricsViewServerConfig);
        return {
          useLiveQuery: (topic, query) => hooks.useLiveQuery(topic, query),
        } satisfies ViewServerMetricsHooks;
      }),
    ).then(
      (hooks) => {
        if (!disposed) {
          setConnection({ status: "ready", hooks });
        }
      },
      (error: unknown) => {
        if (!disposed) {
          setConnection({ status: "error", message: errorMessage(error) });
        }
      },
    );

    return () => {
      disposed = true;
      if (scope !== undefined) {
        void Effect.runPromise(Scope.close(scope, Exit.void));
      }
    };
  }, [rpcUrl]);

  if (connection.status === "ready") {
    return (
      <main className="metrics-app">
        <ViewServerMetricsDashboard hooks={connection.hooks} title="View Server Metrics" />
      </main>
    );
  }

  return (
    <main className="metrics-app__state">
      <section className="metrics-app__panel" data-status={connection.status}>
        <p className="metrics-app__label">{connection.status}</p>
        <h1 className="metrics-app__title">View Server Metrics</h1>
        <p
          className="metrics-app__message"
          role={connection.status === "error" ? "alert" : undefined}
        >
          {connection.status === "error" ? connection.message : "Opening RPC stream"}
        </p>
      </section>
    </main>
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
