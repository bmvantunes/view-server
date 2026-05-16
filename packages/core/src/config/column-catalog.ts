import * as Effect from "effect/Effect";
import { invalidQuery, type ViewServerError } from "../errors.ts";
import type { RuntimeFilterNode, RuntimeGroupedQuery, RuntimeQuery } from "../protocol/index.ts";
import type { Column, ColumnType } from "../snapshot/chdb-sql-compiler.ts";
import type { TopicConfig } from "./define-config.ts";
import {
  schemaFieldDescriptorsForSchema,
  type SchemaFieldDescriptor,
  type SchemaFieldKind,
} from "./schema-introspection.ts";

export type ColumnCatalogField = SchemaFieldDescriptor & {
  readonly sqlType?: ColumnType | undefined;
  readonly filterable: boolean;
  readonly sortable: boolean;
};

export class ColumnCatalog {
  readonly #topic: string;
  readonly #idField: string;
  readonly #fields: readonly ColumnCatalogField[];
  readonly #fieldNames: ReadonlySet<string> | undefined;

  constructor(args: {
    readonly topic: string;
    readonly idField: string;
    readonly fields: readonly ColumnCatalogField[] | undefined;
  }) {
    this.#topic = args.topic;
    this.#idField = args.idField;
    this.#fields = args.fields ?? [];
    this.#fieldNames =
      args.fields === undefined ? undefined : new Set(args.fields.map((field) => field.name));
  }

  get topic(): string {
    return this.#topic;
  }

  get idField(): string {
    return this.#idField;
  }

  get fields(): readonly ColumnCatalogField[] {
    return this.#fields;
  }

  get fieldNames(): ReadonlySet<string> | undefined {
    return this.#fieldNames;
  }

  get literalStringFields(): ReadonlySet<string> {
    return new Set(this.#fields.filter((field) => field.literalString).map((field) => field.name));
  }

  get columns(): readonly Column[] {
    return this.#fields.flatMap((field) =>
      field.sqlType === undefined
        ? []
        : [{ name: field.name, type: field.sqlType, nullable: field.nullable }],
    );
  }

  hasField(field: string): boolean | undefined {
    return this.#fieldNames?.has(field);
  }

  validateQuery(query: RuntimeQuery): Effect.Effect<RuntimeQuery, ViewServerError> {
    return Effect.fnUntraced(function* (catalog: ColumnCatalog) {
      const invalid = catalog.#invalidQueryField(query);
      return invalid === undefined
        ? query
        : yield* Effect.fail(
            invalidQuery(
              catalog.#topic,
              `${invalid.context} field ${invalid.field} is not present in topic schema`,
            ),
          );
    })(this);
  }

  #invalidQueryField(query: RuntimeQuery): InvalidQueryField | undefined {
    if (this.#fieldNames === undefined) {
      return undefined;
    }
    const filterField = this.#invalidFilterField(query.where);
    if (filterField !== undefined) {
      return filterField;
    }
    return isGroupedQuery(query)
      ? this.#invalidGroupedQueryField(query)
      : this.#invalidRawQueryField(query);
  }

  #invalidRawQueryField(query: RuntimeQuery): InvalidQueryField | undefined {
    if (isGroupedQuery(query)) {
      return undefined;
    }
    for (const field of Object.keys(query.fields)) {
      if (!this.#fieldNames?.has(field)) {
        return { context: "Selected", field };
      }
    }
    for (const order of query.orderBy ?? []) {
      if (!this.#fieldNames?.has(order.field)) {
        return { context: "Sort", field: order.field };
      }
    }
    return undefined;
  }

  #invalidGroupedQueryField(query: RuntimeGroupedQuery): InvalidQueryField | undefined {
    for (const field of query.groupBy) {
      if (!this.#fieldNames?.has(field)) {
        return { context: "GroupBy", field };
      }
    }
    for (const aggregate of Object.values(query.aggregates)) {
      if (!this.#fieldNames?.has(aggregate.field)) {
        return { context: "Aggregate", field: aggregate.field };
      }
    }
    const groupedResultFields = new Set([...query.groupBy, ...Object.keys(query.aggregates)]);
    for (const order of query.orderBy ?? []) {
      if (!groupedResultFields.has(order.field)) {
        return { context: "Grouped sort", field: order.field };
      }
    }
    return undefined;
  }

  #invalidFilterField(filter: RuntimeFilterNode | undefined): InvalidQueryField | undefined {
    if (filter === undefined) {
      return undefined;
    }
    if ("conditions" in filter) {
      for (const condition of filter.conditions) {
        const invalid = this.#invalidFilterField(condition);
        if (invalid !== undefined) {
          return invalid;
        }
      }
      return undefined;
    }
    return this.#fieldNames?.has(filter.field)
      ? undefined
      : { context: "Filter", field: filter.field };
  }
}

type InvalidQueryField = {
  readonly context: "Selected" | "Sort" | "Filter" | "GroupBy" | "Aggregate" | "Grouped sort";
  readonly field: string;
};

export function columnCatalogForTopic(topic: string, config: TopicConfig): ColumnCatalog {
  const descriptors = schemaFieldDescriptorsForSchema(config.schema);
  return new ColumnCatalog({
    topic,
    idField: config.id,
    fields: descriptors?.map(toCatalogField),
  });
}

function toCatalogField(descriptor: SchemaFieldDescriptor): ColumnCatalogField {
  const scalar = isScalarKind(descriptor.kind);
  return {
    ...descriptor,
    filterable: scalar,
    sortable: scalar,
    ...(columnTypeForKind(descriptor.kind) === undefined
      ? {}
      : { sqlType: columnTypeForKind(descriptor.kind) }),
  };
}

function isScalarKind(kind: SchemaFieldKind): boolean {
  return kind !== "unknown";
}

function columnTypeForKind(kind: SchemaFieldKind): ColumnType | undefined {
  switch (kind) {
    case "string":
      return "String";
    case "number":
      return "Float64";
    case "bigint":
      return "Int64";
    case "boolean":
      return "UInt8";
    case "bigdecimal":
      return "Decimal(76, 38)";
    case "unknown":
      return undefined;
  }
}

function isGroupedQuery(query: RuntimeQuery): query is RuntimeGroupedQuery {
  return "groupBy" in query && Array.isArray(query.groupBy);
}
