import type { RuntimeRow } from "./query.ts";

export type RuntimeMutation =
  | {
      readonly type: "publish";
      readonly row: unknown;
    }
  | {
      readonly type: "delta-publish";
      readonly patch: RuntimeRow;
    }
  | {
      readonly type: "delete";
      readonly id: string | number;
    };

export type RuntimeMutationOperation = RuntimeMutation["type"];
