import * as Schema from "effect/Schema";
import { defineConfig, type GroupedQuery, type RawQuery } from "@view-server/core";

export const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  desk: Schema.String,
  status: Schema.Literals(["open", "held", "filled"]),
  price: Schema.Number,
  quantity: Schema.Number,
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export type OrderRow = typeof Order.Type;

export const ordersDemoConfig = defineConfig({
  worker: {
    maxQueueDepth: 256,
    mutationLogSize: 25_000,
    maxActivePlans: 24,
    groupedRefreshDebounceMs: 150,
  },
  topics: {
    orders: {
      id: "id",
      schema: Order,
      snapshot: {
        backend: "memory",
      },
    },
  },
});

export const ordersWindowQuery = {
  fields: {
    id: true,
    symbol: true,
    desk: true,
    status: true,
    price: true,
    quantity: true,
    notional: true,
    updatedAt: true,
  },
  where: {
    field: "status",
    comparator: "one_of",
    value: ["open", "held"],
  },
  orderBy: [
    { field: "notional", direction: "desc" },
    { field: "updatedAt", direction: "desc" },
  ],
  limit: 20,
} satisfies RawQuery<
  OrderRow,
  {
    readonly id: true;
    readonly symbol: true;
    readonly desk: true;
    readonly status: true;
    readonly price: true;
    readonly quantity: true;
    readonly notional: true;
    readonly updatedAt: true;
  }
>;

export const ordersByDeskQuery = {
  groupBy: ["desk", "status"],
  aggregates: {
    orders: { aggFunc: "count", field: "id" },
    quantity: { aggFunc: "sum", field: "quantity" },
    notional: { aggFunc: "sum", field: "notional" },
    avgPrice: { aggFunc: "avg", field: "price" },
  },
  orderBy: [{ field: "notional", direction: "desc" }],
  limit: 12,
} satisfies GroupedQuery<
  OrderRow,
  ["desk", "status"],
  {
    readonly orders: { readonly aggFunc: "count"; readonly field: "id" };
    readonly quantity: { readonly aggFunc: "sum"; readonly field: "quantity" };
    readonly notional: { readonly aggFunc: "sum"; readonly field: "notional" };
    readonly avgPrice: { readonly aggFunc: "avg"; readonly field: "price" };
  }
>;

export function resolveViewServerRpcUrl(): string {
  const explicitUrl = import.meta.env.VITE_VIEW_SERVER_RPC_URL;
  if (typeof explicitUrl === "string" && explicitUrl.length > 0) {
    return explicitUrl;
  }
  return "ws://127.0.0.1:3000/rpc";
}

export function makeOrder(index: number, tick = 0): OrderRow {
  const symbol = symbols[index % symbols.length] ?? "AAPL";
  const desk = desks[index % desks.length] ?? "LDN";
  const status = statuses[(index + tick) % statuses.length] ?? "open";
  const price = 90 + ((index * 13 + tick * 7) % 180) + ((index + tick) % 10) / 10;
  const quantity = 10 + ((index * 19 + tick * 3) % 450);
  const notional = Math.round(price * quantity * 100) / 100;
  return {
    id: `order-${index}`,
    symbol,
    desk,
    status,
    price,
    quantity,
    notional,
    updatedAt: 1_700_000_000_000 + tick * 1_000 + index,
  };
}

export function initialOrders(count = 800): readonly OrderRow[] {
  return Array.from({ length: count }, (_, index) => makeOrder(index));
}

export const symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "ORCL", "SHOP"] as const;
export const desks = ["LDN", "NYC", "SFO", "AMS"] as const;
export const statuses = ["open", "held", "filled"] as const;
