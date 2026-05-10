import { Effect } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import type { ViewServerError } from "../errors.ts";
import { ViewServerRuntime, type HealthResponse, type ViewServerRuntimeShape } from "./runtime.ts";

type HealthErrorResponse = {
  readonly ok: false;
  readonly error: {
    readonly tag: string;
    readonly message: string;
  };
};

export const layerViewServerHealthRoutes = HttpRouter.use(
  Effect.fn(function* (router) {
    const runtime = yield* ViewServerRuntime;
    yield* router.add("GET", runtime.config.health.path, healthHttpResponse(runtime, "live"));
    yield* router.add("GET", runtime.config.health.readyPath, healthHttpResponse(runtime, "ready"));
  }),
);

function healthHttpResponse(
  runtime: ViewServerRuntimeShape,
  kind: "live" | "ready",
): Effect.Effect<HttpServerResponse.HttpServerResponse> {
  return runtime.health.pipe(
    Effect.match({
      onFailure: (error) =>
        HttpServerResponse.jsonUnsafe(errorBody(error), {
          status: kind === "ready" ? 503 : 500,
        }),
      onSuccess: (health) =>
        HttpServerResponse.jsonUnsafe(health satisfies HealthResponse, {
          status: kind === "ready" && !health.ok ? 503 : 200,
        }),
    }),
  );
}

function errorBody(error: ViewServerError): HealthErrorResponse {
  return {
    ok: false,
    error: {
      tag: error._tag,
      message: error.message,
    },
  };
}
