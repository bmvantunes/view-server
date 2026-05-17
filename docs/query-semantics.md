# Query Semantics

This document defines the behavior that must match across memory queries, active raw views,
grouped accumulators, client delta application, and chDB snapshots.

chDB-backed snapshots are the production oracle. Memory and active paths exist so the hot path can
stay fast, but they must produce the same rows, row order, aggregate values, `totalRows`, and delta
convergence as the chDB SQL path.

## Ordering

Raw queries append the topic id field as the final ascending tiebreak unless the query already
orders by that field. Grouped queries append every `groupBy` field as ascending tiebreaks unless the
query already orders by those fields.

Nullable values sort before non-null values in ascending order and after non-null values in
descending order. The SQL compiler emits explicit `isNull(...)` order keys so this rule applies to
source columns and nullable aggregate aliases.

String ordering is case-insensitive and matches ClickHouse `lower(toString(field))` binary ordering.
It is not locale collation. Accents and Unicode code points therefore follow the same lower-cased
binary order in memory, active views, grouped accumulators, and chDB.

## Nullish Values

At query boundaries, missing object fields and `undefined` values are materialized as SQL `NULL`.
Memory projections, grouped keys, active view rows, and chDB rows all expose those values as `null`.

Filter equality treats `null`, `undefined`, and missing fields as the same nullish value. Relational
filters against nullish values do not match. `one_of` handles nullish candidates with `isNull(...)`
in SQL and the same nullish check in memory.

`-0` is materialized as `0` because the JSON/chDB mirror path cannot preserve JavaScript negative
zero through row payloads.

## Aggregates

Aggregates follow ClickHouse NULL behavior:

- `count()` counts rows.
- `sum`, `avg`, `min`, and `max` ignore nullish values and return `null` when a group has no
  non-null value.
- `count_distinct` ignores nullish values.
- `string_concat` and `string_concat_distinct` ignore nullish values and return an empty string when
  no value exists.
- BigDecimal aggregates are encoded as `Decimal(76, 38)` and decoded back to `Schema.BigDecimal`
  values.

## Unsupported Values

Non-finite JavaScript numbers (`NaN`, `Infinity`, `-Infinity`) are not part of the query contract.
Topic schemas should reject them before rows reach the runtime. Query parity tests intentionally do
not normalize non-finite numbers into valid values.

## Intentional Divergences

There are no intentional query-result semantic divergences from the chDB SQL path. Memory queries,
active raw views, grouped accumulators, and client-applied deltas must match chDB snapshots.

The only materialization constraint currently documented is JavaScript `-0` becoming `0` at the
row serialization boundary. Non-finite numbers are unsupported inputs, not alternate semantics.

## Parity Guard

`packages/core/tests/query-semantics-parity.test.ts` compares:

- memory raw/grouped query execution,
- active raw views,
- grouped accumulators when eligible,
- chDB snapshots,
- client-visible rows after applying per-mutation and coalesced multi-mutation deltas.

The test preserves row order. It only normalizes representation details such as BigDecimal display
strings and bigint display strings in failure output. If chDB and memory disagree, the implementation
must change or the compiler must explicitly reject the unsupported query shape.
