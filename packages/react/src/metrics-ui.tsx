import { VIEW_SERVER_HEALTH_TOPIC } from "@view-server/core/config";
import {
  metricsDashboardViewModel,
  metricsValueFromResult,
  viewServerHealthQuery,
  type ViewServerMetricsHooks,
} from "./metrics-view-model.ts";

export {
  viewServerHealthQuery,
  type ViewServerMetricsHooks,
  type ViewServerMetricsRow,
} from "./metrics-view-model.ts";

export function ViewServerMetricsDashboard(props: {
  readonly hooks: ViewServerMetricsHooks;
  readonly title?: string | undefined;
}) {
  const result = props.hooks.useLiveQuery(VIEW_SERVER_HEALTH_TOPIC, viewServerHealthQuery);
  const value = metricsValueFromResult(result);
  const viewModel = metricsDashboardViewModel(value);
  const status = viewModel.status;

  return (
    <section className="vs-metrics" data-status={status}>
      <style>{metricsDashboardCss}</style>
      <header className="vs-metrics__header">
        <div>
          <p className="vs-metrics__eyebrow">View Server Ops</p>
          <h1>{props.title ?? "Realtime view control"}</h1>
        </div>
        <div className="vs-metrics__status" data-status={status}>
          <span aria-hidden="true" />
          <strong>{status}</strong>
          <small>{viewModel.liveStatus}</small>
        </div>
      </header>

      <div className="vs-metrics__summary" aria-label="Server summary">
        {viewModel.summary.map((cell) => (
          <MetricCell key={cell.label} label={cell.label} value={cell.value} />
        ))}
      </div>

      <div className="vs-metrics__latency" aria-label="Latency signals">
        {viewModel.latency.map((cell) => (
          <SignalCell key={cell.label} label={cell.label} value={cell.value} />
        ))}
      </div>

      <div className="vs-metrics__topics" aria-label="Topic health">
        <div className="vs-metrics__topic-head">
          <span>topic</span>
          <span>status</span>
          <span>chDB</span>
          <span>rows</span>
          <span>subs</span>
          <span>queue</span>
          <span>sub lag</span>
          <span>plans</span>
          <span>builds</span>
          <span>pending</span>
          <span>views</span>
          <span>fallbacks</span>
          <span>skipped</span>
          <span>indexed rows</span>
          <span>index</span>
          <span>lag</span>
          <span>chDB req</span>
          <span>restarts</span>
          <span>chDB ver</span>
          <span>pid</span>
          <span>updated</span>
        </div>
        {viewModel.topics.map((topic) => (
          <div className="vs-metrics__topic-row" data-status={topic.status} key={topic.id}>
            <strong>{topic.topic}</strong>
            <span>{topic.status}</span>
            <span title={topic.chdbTitle}>{topic.chdbStatus}</span>
            <span>{topic.rows}</span>
            <span>{topic.subscribers}</span>
            <span>{topic.queueDepth}</span>
            <span>{topic.maxSubscriptionLagVersions}</span>
            <span>{topic.activePlanCount}</span>
            <span>{topic.activePlanBuilds}</span>
            <span>{topic.activePlanPendingCount}</span>
            <span>{topic.activeViewCount}</span>
            <span>{topic.activePlanFallbackCount}</span>
            <span>{topic.activePlanAutoBuildSkippedCount}</span>
            <span>{topic.activePlanRows}</span>
            <span>{topic.activePlanIndexEstimatedBytes}</span>
            <span>{topic.kafkaLagTotal}</span>
            <span>{topic.chdbPendingRequests}</span>
            <span>{topic.chdbRestarts}</span>
            <span>{topic.chdbBackendVersion}</span>
            <span>{topic.chdbPid}</span>
            <span>{topic.updatedAt}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function MetricCell(props: { readonly label: string; readonly value: string }) {
  return (
    <div className="vs-metrics__metric">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function SignalCell(props: { readonly label: string; readonly value: string }) {
  return (
    <div className="vs-metrics__signal">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

export const metricsDashboardCss = `
.vs-metrics {
  color-scheme: dark;
  width: min(1180px, calc(100vw - 24px));
  margin: 0 auto;
  padding: 20px;
  color: #e8eee9;
  background:
    linear-gradient(90deg, rgba(217, 255, 129, 0.08) 1px, transparent 1px),
    linear-gradient(rgba(140, 169, 151, 0.08) 1px, transparent 1px),
    #0a0e0b;
  background-size: 44px 44px;
  border: 1px solid rgba(154, 181, 164, 0.24);
  font-family: "IBM Plex Mono", "Berkeley Mono", "Aptos Mono", "SFMono-Regular", monospace;
}

.vs-metrics * {
  box-sizing: border-box;
}

.vs-metrics__header {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: end;
  gap: 16px;
  padding-bottom: 18px;
  border-bottom: 1px solid rgba(154, 181, 164, 0.24);
}

.vs-metrics__eyebrow,
.vs-metrics__metric span,
.vs-metrics__signal span,
.vs-metrics__topic-head {
  margin: 0;
  color: #9aab9f;
  font-size: 11px;
  line-height: 1.2;
  text-transform: uppercase;
  letter-spacing: 0;
}

.vs-metrics h1 {
  max-width: 820px;
  margin: 8px 0 0;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 64px;
  font-weight: 500;
  line-height: 0.92;
}

.vs-metrics__status {
  display: grid;
  grid-template-columns: 10px auto;
  align-items: center;
  gap: 8px;
  min-width: 150px;
  padding: 10px 12px;
  border: 1px solid rgba(217, 255, 129, 0.3);
  background: rgba(217, 255, 129, 0.08);
}

.vs-metrics__status span {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: #d9ff81;
  box-shadow: 0 0 18px rgba(217, 255, 129, 0.72);
}

.vs-metrics__status strong {
  font-size: 13px;
  line-height: 1;
  text-transform: uppercase;
}

.vs-metrics__status small {
  grid-column: 2;
  color: #9aab9f;
  font-size: 11px;
}

.vs-metrics [data-status="degraded"] .vs-metrics__status,
.vs-metrics__status[data-status="degraded"],
.vs-metrics__topic-row[data-status="degraded"] {
  border-color: rgba(255, 197, 92, 0.38);
}

.vs-metrics [data-status="degraded"] .vs-metrics__status span,
.vs-metrics__status[data-status="degraded"] span {
  background: #ffc55c;
  box-shadow: 0 0 18px rgba(255, 197, 92, 0.72);
}

.vs-metrics [data-status="stopping"] .vs-metrics__status,
.vs-metrics__status[data-status="stopping"],
.vs-metrics__topic-row[data-status="stopping"] {
  border-color: rgba(255, 101, 101, 0.42);
}

.vs-metrics [data-status="stopping"] .vs-metrics__status span,
.vs-metrics__status[data-status="stopping"] span {
  background: #ff6565;
  box-shadow: 0 0 18px rgba(255, 101, 101, 0.68);
}

.vs-metrics__summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(126px, 1fr));
  gap: 1px;
  margin-top: 16px;
  background: rgba(154, 181, 164, 0.24);
}

.vs-metrics__metric {
  min-height: 124px;
  padding: 16px;
  background: rgba(10, 14, 11, 0.92);
}

.vs-metrics__metric strong {
  display: block;
  margin-top: 28px;
  font-size: 54px;
  font-weight: 600;
  line-height: 0.9;
}

.vs-metrics__latency {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(126px, 1fr));
  gap: 8px;
  margin-top: 12px;
}

.vs-metrics__signal {
  min-height: 74px;
  padding: 12px;
  border: 1px solid rgba(154, 181, 164, 0.18);
  background: rgba(12, 19, 15, 0.82);
}

.vs-metrics__signal strong {
  display: block;
  margin-top: 14px;
  color: #d9ff81;
  font-size: 22px;
  font-weight: 600;
}

.vs-metrics__topics {
  margin-top: 14px;
  border: 1px solid rgba(154, 181, 164, 0.24);
  background: rgba(8, 12, 9, 0.86);
  overflow: hidden;
}

.vs-metrics__topic-head,
.vs-metrics__topic-row {
  display: grid;
  grid-template-columns: minmax(140px, 1.4fr) 92px 104px repeat(17, minmax(70px, 0.7fr)) 120px;
  gap: 10px;
  align-items: center;
  min-height: 42px;
  padding: 0 12px;
}

.vs-metrics__topic-head {
  background: rgba(217, 255, 129, 0.08);
}

.vs-metrics__topic-row {
  border-top: 1px solid rgba(154, 181, 164, 0.14);
  font-size: 13px;
}

.vs-metrics__topic-row strong {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (max-width: 760px) {
  .vs-metrics {
    padding: 14px;
  }

  .vs-metrics h1 {
    font-size: 36px;
  }

  .vs-metrics__header,
  .vs-metrics__summary,
  .vs-metrics__latency {
    grid-template-columns: 1fr;
  }

  .vs-metrics__metric {
    min-height: 92px;
  }

  .vs-metrics__metric strong {
    margin-top: 16px;
    font-size: 38px;
  }

  .vs-metrics__topics {
    overflow-x: auto;
  }

  .vs-metrics__topic-head,
  .vs-metrics__topic-row {
    min-width: 1480px;
  }
}
`;
