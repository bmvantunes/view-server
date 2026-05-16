import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { columnCatalogForTopic } from "../src/config/index.ts";
import type { RuntimeQuery } from "../src/protocol/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  symbol: Schema.String,
  price: Schema.BigDecimal,
  quantity: Schema.Number,
  updatedAt: Schema.BigInt,
  archived: Schema.Boolean,
  venue: Schema.optional(Schema.String),
});

const topicConfig = {
  id: "id",
  schema: Order,
};

describe("ColumnCatalog", () => {
  it("derives id, literal strings, field names, and SQL column metadata from the schema", () => {
    const catalog = columnCatalogForTopic("orders", topicConfig);

    expect(catalog.idField).toBe("id");
    expect(catalog.hasField("id")).toBe(true);
    expect(catalog.hasField("missing")).toBe(false);
    expect(catalog.literalStringFields).toEqual(new Set(["status"]));
    expect(catalog.columns).toEqual(
      expect.arrayContaining([
        { name: "id", type: "String", nullable: false },
        { name: "status", type: "String", nullable: false },
        { name: "price", type: "Decimal(76, 38)", nullable: false },
        { name: "quantity", type: "Float64", nullable: false },
        { name: "updatedAt", type: "Int64", nullable: false },
        { name: "archived", type: "UInt8", nullable: false },
        { name: "venue", type: "String", nullable: true },
      ]),
    );
  });

  it.effect("rejects invalid raw query fields as typed query errors", () =>
    Effect.gen(function* () {
      const catalog = columnCatalogForTopic("orders", topicConfig);

      yield* expectInvalidField(catalog, {
        fields: { missing: true },
        limit: 10,
      });
      yield* expectInvalidField(catalog, {
        fields: { id: true },
        where: {
          field: "missing",
          comparator: "equals",
          value: "x",
        },
        limit: 10,
      });
      yield* expectInvalidField(catalog, {
        fields: { id: true },
        orderBy: [{ field: "missing", direction: "asc" }],
        limit: 10,
      });
    }),
  );

  it.effect("rejects invalid grouped fields while allowing aggregate sort aliases", () =>
    Effect.gen(function* () {
      const catalog = columnCatalogForTopic("orders", topicConfig);

      yield* expectInvalidField(catalog, {
        groupBy: ["missing"],
        aggregates: {
          trades: { aggFunc: "count", field: "id" },
        },
        limit: 10,
      });
      yield* expectInvalidField(catalog, {
        groupBy: ["symbol"],
        aggregates: {
          total: { aggFunc: "sum", field: "missing" },
        },
        limit: 10,
      });
      yield* expectInvalidField(catalog, {
        groupBy: ["symbol"],
        aggregates: {
          total: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "missing", direction: "desc" }],
        limit: 10,
      });

      const valid = {
        groupBy: ["symbol"],
        aggregates: {
          total: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "total", direction: "desc" }],
        where: {
          field: "status",
          comparator: "equals",
          value: "open",
        },
        limit: 10,
      } satisfies RuntimeQuery;

      expect(yield* catalog.validateQuery(valid)).toBe(valid);
    }),
  );
});

function expectInvalidField(
  catalog: ReturnType<typeof columnCatalogForTopic>,
  query: RuntimeQuery,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const error = yield* catalog.validateQuery(query).pipe(Effect.flip, Effect.orDie);
    expect(error._tag).toBe("InvalidQuery");
    expect(error.message).toContain("is not present in topic schema");
  });
}
