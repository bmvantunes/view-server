import type * as Schema from "effect/Schema";
import type { RowObject } from "./define-config.ts";

export function literalStringFieldsForSchema(
  schema: Schema.Decoder<RowObject, never>,
): ReadonlySet<string> {
  const fields = new Set<string>();
  const signatures = propertySignaturesForSchema(schema);
  if (signatures === undefined) {
    return fields;
  }
  for (const signature of signatures) {
    if (!isRecord(signature) || typeof signature.name !== "string") {
      continue;
    }
    if (isStringLiteralOnlyAst(signature.type)) {
      fields.add(signature.name);
    }
  }
  return fields;
}

export function fieldNamesForSchema(
  schema: Schema.Decoder<RowObject, never>,
): ReadonlySet<string> | undefined {
  const signatures = propertySignaturesForSchema(schema);
  if (signatures === undefined) {
    return undefined;
  }
  const fields = new Set<string>();
  for (const signature of signatures) {
    if (isRecord(signature) && typeof signature.name === "string") {
      fields.add(signature.name);
    }
  }
  return fields;
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

function isUndefinedAst(ast: unknown): boolean {
  return isRecord(ast) && ast._tag === "Undefined";
}

function hasAst(value: unknown): value is { readonly ast: unknown } {
  return isRecord(value) && "ast" in value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
