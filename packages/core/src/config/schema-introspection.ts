import type * as Schema from "effect/Schema";
import type { RowObject } from "./define-config.ts";

export type SchemaFieldKind = "string" | "number" | "bigint" | "boolean" | "bigdecimal" | "unknown";

export type SchemaFieldDescriptor = {
  readonly name: string;
  readonly kind: SchemaFieldKind;
  readonly nullable: boolean;
  readonly literalString: boolean;
};

export function schemaFieldDescriptorsForSchema(
  schema: Schema.Decoder<RowObject, never>,
): readonly SchemaFieldDescriptor[] | undefined {
  const signatures = propertySignaturesForSchema(schema);
  if (signatures === undefined) {
    return undefined;
  }
  return signatures.flatMap((signature) => {
    if (!isRecord(signature) || typeof signature.name !== "string") {
      return [];
    }
    return [
      {
        name: signature.name,
        kind: schemaFieldKind(signature.type),
        nullable: astAllowsNullish(signature.type),
        literalString: isStringLiteralOnlyAst(signature.type),
      },
    ];
  });
}

export function literalStringFieldsForSchema(
  schema: Schema.Decoder<RowObject, never>,
): ReadonlySet<string> {
  const fields = new Set<string>();
  const descriptors = schemaFieldDescriptorsForSchema(schema);
  if (descriptors === undefined) {
    return fields;
  }
  for (const descriptor of descriptors) {
    if (descriptor.literalString) {
      fields.add(descriptor.name);
    }
  }
  return fields;
}

export function fieldNamesForSchema(
  schema: Schema.Decoder<RowObject, never>,
): ReadonlySet<string> | undefined {
  const descriptors = schemaFieldDescriptorsForSchema(schema);
  if (descriptors === undefined) {
    return undefined;
  }
  return new Set(descriptors.map((descriptor) => descriptor.name));
}

export function schemaHasField(
  schema: Schema.Decoder<RowObject, never>,
  field: string,
): boolean | undefined {
  return fieldNamesForSchema(schema)?.has(field);
}

function propertySignaturesForSchema(
  schema: Schema.Decoder<RowObject, never>,
): readonly unknown[] | undefined {
  if (!hasAst(schema) || !isRecord(schema.ast) || schema.ast._tag !== "Objects") {
    return undefined;
  }
  const propertySignatures = schema.ast.propertySignatures;
  return Array.isArray(propertySignatures) ? propertySignatures : undefined;
}

function isStringLiteralOnlyAst(ast: unknown): boolean {
  if (!isRecord(ast)) {
    return false;
  }
  if (ast._tag === "Literal") {
    return typeof ast.literal === "string";
  }
  if (ast._tag !== "Union" || !Array.isArray(ast.types)) {
    return false;
  }
  const nonUndefinedTypes = ast.types.filter((type) => !isUndefinedAst(type));
  return nonUndefinedTypes.length > 0 && nonUndefinedTypes.every(isStringLiteralOnlyAst);
}

function astAllowsNullish(ast: unknown): boolean {
  if (!isRecord(ast)) {
    return false;
  }
  if (isUndefinedAst(ast) || ast._tag === "Null") {
    return true;
  }
  if (ast._tag === "Union" && Array.isArray(ast.types)) {
    return ast.types.some(astAllowsNullish);
  }
  return false;
}

function schemaFieldKind(ast: unknown): SchemaFieldKind {
  if (!isRecord(ast)) {
    return "unknown";
  }
  if (ast._tag === "Union" && Array.isArray(ast.types)) {
    const nonNullishTypes = ast.types.filter((type) => !astAllowsNullish(type));
    const kinds = new Set(nonNullishTypes.map(schemaFieldKind));
    if (kinds.size !== 1) {
      return "unknown";
    }
    for (const kind of kinds) {
      return kind;
    }
    return "unknown";
  }
  if (ast._tag === "Literal") {
    switch (typeof ast.literal) {
      case "string":
        return "string";
      case "number":
        return "number";
      case "bigint":
        return "bigint";
      case "boolean":
        return "boolean";
      default:
        return "unknown";
    }
  }
  switch (ast._tag) {
    case "String":
      return "string";
    case "Number":
      return "number";
    case "BigInt":
      return "bigint";
    case "Boolean":
      return "boolean";
    case "Declaration":
      return isBigDecimalAst(ast) ? "bigdecimal" : "unknown";
    default:
      return "unknown";
  }
}

function isBigDecimalAst(ast: Readonly<Record<string, unknown>>): boolean {
  const annotations = ast.annotations;
  if (!isRecord(annotations)) {
    return false;
  }
  const typeConstructor = annotations.typeConstructor;
  return isRecord(typeConstructor) && typeConstructor._tag === "effect/BigDecimal";
}

function isUndefinedAst(ast: unknown): boolean {
  return isRecord(ast) && ast._tag === "Undefined";
}

function hasAst(value: unknown): value is { readonly ast: unknown } {
  return isRecord(value) && "ast" in value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
