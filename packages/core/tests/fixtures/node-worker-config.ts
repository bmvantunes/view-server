import * as Schema from "effect/Schema";
import { defineConfig } from "../../src/config/index.ts";

export const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

export default defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});
