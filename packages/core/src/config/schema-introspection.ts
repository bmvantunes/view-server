import type { Schema } from "effect";
import type { RowObject } from "./define-config.ts";

export function literalStringFieldsForSchema(
  schema: Schema.Decoder<RowObject, never>,
): ReadonlySet<string> {
  const fields = new Set<string>();
  if (!hasAst(schema) || !isRecord(schema.ast) || schema.ast._tag !== "Objects") {
    return fields;
  }
  const propertySignatures = schema.ast.propertySignatures;
  if (!Array.isArray(propertySignatures)) {
    return fields;
  }
  for (const signature of propertySignatures) {
    if (!isRecord(signature) || typeof signature.name !== "string") {
      continue;
    }
    if (isStringLiteralOnlyAst(signature.type)) {
      fields.add(signature.name);
    }
  }
  return fields;
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
