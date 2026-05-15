import * as Schema from "effect/Schema";
import { defineConfig } from "../../src/config/index.ts";

export const productionConfig = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Schema.Struct({
        id: Schema.String,
        symbol: Schema.String,
        price: Schema.Number,
      }),
    },
  },
});

export default productionConfig;
