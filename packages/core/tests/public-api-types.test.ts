import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { defineConfig, type TopicName } from "@view-server/core/config";
import type { GroupedQuery, InferQueryResult, RawQuery } from "@view-server/core/query";
import type { LiveQueryInitialData, ViewServerClient } from "@view-server/core/client";
import type { ViewServerError } from "@view-server/core/errors";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  quantity: Schema.Number,
  amount: Schema.BigDecimal,
});

type OrderRow = typeof Order.Type;

const Trade = Schema.Struct({
  tradeId: Schema.String,
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  notional: Schema.BigDecimal,
});

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
    trades: {
      id: "tradeId",
      schema: Trade,
    },
  },
});

const rawQuery = {
  fields: {
    id: true,
    amount: true,
  },
  where: {
    field: "amount",
    comparator: "greater_than",
    value: BigDecimal.fromStringUnsafe("10.00"),
  },
  orderBy: [{ field: "amount", direction: "desc" }],
  limit: 10,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly amount: true }>;

const groupedQuery = {
  groupBy: ["status"],
  aggregates: {
    amountTotal: {
      aggFunc: "sum",
      field: "amount",
    },
  },
} satisfies GroupedQuery<
  OrderRow,
  readonly ["status"],
  {
    readonly amountTotal: {
      readonly aggFunc: "sum";
      readonly field: "amount";
    };
  }
>;

type Equal<TLeft, TRight> =
  (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2 ? true : false;
type Expect<TValue extends true> = TValue;

type _TopicNamesAreInferred = Expect<Equal<TopicName<typeof config>, "orders" | "trades">>;
type _RawRowsAreProjected = Expect<
  Equal<
    InferQueryResult<typeof config, "orders", typeof rawQuery>[number],
    {
      readonly id: string;
      readonly amount: BigDecimal.BigDecimal;
    }
  >
>;
type GroupedResultRow = InferQueryResult<typeof config, "orders", typeof groupedQuery>[number];
type _GroupedStatusStaysStrict = Expect<Equal<GroupedResultRow["status"], "open" | "closed">>;
type _BigDecimalAggregatesStayDecimal = Expect<
  Equal<GroupedResultRow["amountTotal"], BigDecimal.BigDecimal>
>;

function typedClientSamples(
  client: ViewServerClient<typeof config>,
): Effect.Effect<void, ViewServerError> {
  return Effect.gen(function* () {
    yield* client.publish("orders", {
      id: "o-1",
      status: "open",
      quantity: 10,
      amount: BigDecimal.fromStringUnsafe("12.50"),
    });

    yield* client.deltaPublish("orders", {
      id: "o-1",
      quantity: 11,
    });

    yield* client.publish("trades", {
      tradeId: "t-1",
      symbol: "AAPL",
      side: "buy",
      notional: BigDecimal.fromStringUnsafe("1000.00"),
    });

    const result: LiveQueryInitialData<
      InferQueryResult<typeof config, "orders", typeof rawQuery>[number]
    > = yield* client.query("orders", rawQuery);
    expect(result.totalRows).toBeTypeOf("number");

    // @ts-expect-error topic names are inferred from defineConfig.
    yield* client.query("customers", rawQuery);

    // @ts-expect-error publish requires the full row shape.
    yield* client.publish("orders", {
      id: "o-2",
      status: "open",
      quantity: 10,
    });

    // @ts-expect-error deltaPublish requires the topic id field.
    yield* client.deltaPublish("orders", {
      quantity: 12,
    });

    yield* client.deltaPublish("orders", {
      id: "o-1",
      // @ts-expect-error literal unions stay strict.
      status: "OPEN",
    });
  });
}

function invalidQuerySamples() {
  expect({
    fields: {
      id: true,
    },
    // @ts-expect-error numeric filters reject strings.
    where: {
      field: "quantity",
      comparator: "greater_than",
      value: "10",
    },
  } satisfies RawQuery<OrderRow, { readonly id: true }>).toBeDefined();

  expect({
    fields: {
      id: true,
    },
    // @ts-expect-error BigDecimal filters require BigDecimal values.
    where: {
      field: "amount",
      comparator: "greater_than",
      value: 10,
    },
  } satisfies RawQuery<OrderRow, { readonly id: true }>).toBeDefined();

  expect({
    fields: {
      id: true,
    },
    orderBy: [
      {
        // @ts-expect-error order fields are limited to row fields.
        field: "missing",
        direction: "asc",
      },
    ],
  } satisfies RawQuery<OrderRow, { readonly id: true }>).toBeDefined();

  expect({
    // @ts-expect-error group fields are limited to row fields.
    groupBy: ["status", "missing"],
    aggregates: {
      rows: {
        aggFunc: "count",
        field: "id",
      },
    },
  } satisfies GroupedQuery<OrderRow>).toBeDefined();

  expect({
    groupBy: ["status"],
    aggregates: {
      // @ts-expect-error sum aggregates require numeric or BigDecimal fields.
      invalidTotal: {
        aggFunc: "sum",
        field: "status",
      },
    },
  } satisfies GroupedQuery<OrderRow>).toBeDefined();
}

describe("public API type contracts", () => {
  it("keeps config-derived client and query types strict", () => {
    expect(typeof typedClientSamples).toBe("function");
    expect(typeof invalidQuerySamples).toBe("function");
  });
});
