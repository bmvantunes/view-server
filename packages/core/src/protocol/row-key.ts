import {
  decodeStableKeyFromWire,
  encodeStableKeyForWire,
  stableKeyEquals,
  stableKeyFromRow,
  type StableKey,
} from "./stable-key.ts";

export type RowKey = {
  readonly idField: string;
  readonly get: (row: Readonly<Record<string, unknown>>) => StableKey;
  readonly equals: (left: StableKey, right: StableKey) => boolean;
  readonly encodeForWire: (key: StableKey) => StableKey;
  readonly decodeFromWire: (value: unknown) => StableKey;
};

export function makeRowKey(idField: string): RowKey {
  return {
    idField,
    get: (row) => stableKeyFromRow(row, idField),
    equals: stableKeyEquals,
    encodeForWire: encodeStableKeyForWire,
    decodeFromWire: decodeStableKeyFromWire,
  };
}

export function rowKeyFromTopicConfig(config: { readonly id: string }): RowKey {
  return makeRowKey(config.id);
}
