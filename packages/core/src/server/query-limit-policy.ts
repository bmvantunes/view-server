import * as Effect from "effect/Effect";
import type {
  ColumnCatalog,
  NormalizedQueryLimits,
  NormalizedViewServerConfig,
  QueryLimitsConfig,
} from "../config/index.ts";
import { invalidQuery, queryLimitExceeded, type ViewServerError } from "../errors.ts";
import type { RuntimeFilterNode, RuntimeGroupedQuery, RuntimeQuery } from "../protocol/index.ts";
import { isRuntimeGroupedQuery } from "../protocol/index.ts";

export type QueryLimitPolicyMetrics = {
  readonly rejectedQueries: number;
  readonly rejectedQueriesByTopic: Readonly<Record<string, number>>;
};

export class QueryLimitPolicy {
  readonly #globalLimits: NormalizedQueryLimits;
  readonly #topicLimits: Readonly<Record<string, QueryLimitsConfig | undefined>>;
  readonly #rejectedByTopic = new Map<string, number>();

  constructor(args: {
    readonly globalLimits: NormalizedQueryLimits;
    readonly topicLimits: Readonly<Record<string, QueryLimitsConfig | undefined>>;
  }) {
    this.#globalLimits = args.globalLimits;
    this.#topicLimits = args.topicLimits;
  }

  static fromConfig(config: NormalizedViewServerConfig): QueryLimitPolicy {
    return new QueryLimitPolicy({
      globalLimits: config.limits,
      topicLimits: Object.fromEntries(
        Object.entries(config.topics).map(([topic, topicConfig]) => [topic, topicConfig.limits]),
      ),
    });
  }

  validate(
    topic: string,
    query: RuntimeQuery,
    catalog: ColumnCatalog | undefined,
  ): Effect.Effect<RuntimeQuery, ViewServerError> {
    return Effect.fn("view-server.query_limit.validate")(function* (
      policy: QueryLimitPolicy,
      targetTopic: string,
      targetQuery: RuntimeQuery,
      targetCatalog: ColumnCatalog | undefined,
    ) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": targetTopic,
      });
      const limits = policy.#limitsForTopic(targetTopic);
      if (
        targetQuery.offset !== undefined &&
        (!Number.isInteger(targetQuery.offset) || targetQuery.offset < 0)
      ) {
        return yield* policy.#reject(
          targetTopic,
          invalidQuery(targetTopic, "Query offset must be a non-negative integer"),
        );
      }
      if (
        targetQuery.limit !== undefined &&
        (!Number.isInteger(targetQuery.limit) || targetQuery.limit <= 0)
      ) {
        return yield* policy.#reject(
          targetTopic,
          invalidQuery(targetTopic, "Query limit must be a positive integer"),
        );
      }
      const limitedQuery: RuntimeQuery =
        targetQuery.limit === undefined
          ? { ...targetQuery, limit: limits.maxPageSize }
          : targetQuery;
      if (limitedQuery.limit !== undefined && limitedQuery.limit > limits.maxPageSize) {
        return yield* policy.#reject(
          targetTopic,
          queryLimitExceeded(targetTopic, "maxPageSize", limits.maxPageSize, limitedQuery.limit),
        );
      }
      if (isRuntimeGroupedQuery(limitedQuery)) {
        yield* policy.#validateGroupedQuery(targetTopic, limitedQuery, limits);
      }
      const filterStats = runtimeFilterStats(limitedQuery.where);
      if (filterStats.depth > limits.maxFilterDepth) {
        return yield* policy.#reject(
          targetTopic,
          queryLimitExceeded(
            targetTopic,
            "maxFilterDepth",
            limits.maxFilterDepth,
            filterStats.depth,
          ),
        );
      }
      if (filterStats.conditions > limits.maxFilterConditions) {
        return yield* policy.#reject(
          targetTopic,
          queryLimitExceeded(
            targetTopic,
            "maxFilterConditions",
            limits.maxFilterConditions,
            filterStats.conditions,
          ),
        );
      }
      if (targetCatalog !== undefined) {
        return yield* targetCatalog.validateQuery(limitedQuery);
      }
      return limitedQuery;
    })(this, topic, query, catalog);
  }

  rejectedCount(topic: string): number {
    return this.#rejectedByTopic.get(topic) ?? 0;
  }

  metrics(): QueryLimitPolicyMetrics {
    return {
      rejectedQueries: Array.from(this.#rejectedByTopic.values()).reduce(
        (sum, count) => sum + count,
        0,
      ),
      rejectedQueriesByTopic: Object.fromEntries(this.#rejectedByTopic),
    };
  }

  #validateGroupedQuery(
    topic: string,
    query: RuntimeGroupedQuery,
    limits: NormalizedQueryLimits,
  ): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (policy: QueryLimitPolicy) {
      if (query.groupBy.length > limits.maxGroupByFields) {
        return yield* policy.#reject(
          topic,
          queryLimitExceeded(
            topic,
            "maxGroupByFields",
            limits.maxGroupByFields,
            query.groupBy.length,
          ),
        );
      }
      const aggregateCount = Object.keys(query.aggregates).length;
      if (aggregateCount > limits.maxAggregateCount) {
        return yield* policy.#reject(
          topic,
          queryLimitExceeded(topic, "maxAggregateCount", limits.maxAggregateCount, aggregateCount),
        );
      }
    })(this);
  }

  #reject(topic: string, error: ViewServerError): Effect.Effect<never, ViewServerError> {
    this.#rejectedByTopic.set(topic, this.rejectedCount(topic) + 1);
    return Effect.fail(error);
  }

  #limitsForTopic(topic: string): NormalizedQueryLimits {
    const overrides = this.#topicLimits[topic];
    return {
      maxPageSize: overrides?.maxPageSize ?? this.#globalLimits.maxPageSize,
      maxAggregateCount: overrides?.maxAggregateCount ?? this.#globalLimits.maxAggregateCount,
      maxGroupByFields: overrides?.maxGroupByFields ?? this.#globalLimits.maxGroupByFields,
      maxFilterDepth: overrides?.maxFilterDepth ?? this.#globalLimits.maxFilterDepth,
      maxFilterConditions: overrides?.maxFilterConditions ?? this.#globalLimits.maxFilterConditions,
    };
  }
}

export function runtimeFilterStats(node: RuntimeFilterNode | undefined): {
  readonly depth: number;
  readonly conditions: number;
} {
  if (node === undefined) {
    return { depth: 0, conditions: 0 };
  }
  if ("conditions" in node) {
    const childStats = node.conditions.map(runtimeFilterStats);
    return {
      depth: 1 + childStats.reduce((max, stats) => Math.max(max, stats.depth), 0),
      conditions: childStats.reduce((sum, stats) => sum + stats.conditions, 0),
    };
  }
  return { depth: 1, conditions: 1 };
}
