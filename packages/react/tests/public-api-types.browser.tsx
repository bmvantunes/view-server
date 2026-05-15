import { describe, expect, test } from "vite-plus/test";
import * as BigDecimal from "effect/BigDecimal";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/unstable/reactivity";
import { defineConfig } from "@view-server/core/config";
import type { RawQuery } from "@view-server/core/query";
import type { LiveQueryResult } from "@view-server/core/client";
import { createViewServerReact } from "@view-server/react";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  amount: Schema.BigDecimal,
});

type OrderRow = typeof Order.Type;

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const query = {
  fields: {
    id: true,
    amount: true,
  },
} satisfies RawQuery<OrderRow, { readonly id: true; readonly amount: true }>;

const react = createViewServerReact(config);

function useLiveQueryTypeSample(): LiveQueryResult<{
  readonly id: string;
  readonly amount: BigDecimal.BigDecimal;
}> {
  const result = react.useLiveQuery("orders", query, {
    rows: [
      {
        id: "o-1",
        amount: BigDecimal.fromStringUnsafe("12.50"),
      },
    ],
    totalRows: 1,
  });

  if (AsyncResult.isSuccess(result)) {
    expect(result.value.rows[0]?.amount).toBeDefined();
  }

  return result;
}

describe("react public API type contracts", () => {
  test("returns AsyncResult with config-derived live query rows", () => {
    expect(typeof useLiveQueryTypeSample).toBe("function");
  });
});
