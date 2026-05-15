import { AsyncResult } from "effect/unstable/reactivity";
import type { InferReadableQueryResult, LiveQueryResult } from "@view-server/core";
import { createViewServerReact } from "@view-server/react";
import {
  ordersByDeskQuery,
  ordersDemoConfig,
  ordersWindowQuery,
  resolveViewServerRpcUrl,
} from "./view-server.ts";

const { ViewServerProvider, useLiveQuery } = createViewServerReact(ordersDemoConfig);

type OrderWindowRow = InferReadableQueryResult<
  typeof ordersDemoConfig,
  "orders",
  typeof ordersWindowQuery
>[number];

type DeskMetricRow = InferReadableQueryResult<
  typeof ordersDemoConfig,
  "orders",
  typeof ordersByDeskQuery
>[number];

export function App() {
  return (
    <ViewServerProvider url={resolveViewServerRpcUrl()}>
      <OrdersWorkspace />
    </ViewServerProvider>
  );
}

function OrdersWorkspace() {
  const orders = useLiveQuery("orders", ordersWindowQuery);
  const grouped = useLiveQuery("orders", ordersByDeskQuery);
  const summary = liveSummary(orders);

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Realtime materialized view server</p>
          <h1>Orders Live View</h1>
        </div>
        <div className="connection" data-status={summary.status}>
          <span className="connection__dot" />
          <span>{summary.label}</span>
        </div>
      </header>

      <section className="metrics-strip" aria-label="Live query metrics">
        <Metric label="Rows" value={formatInteger(summary.totalRows)} />
        <Metric label="Visible" value={formatInteger(summary.visibleRows)} />
        <Metric label="Attempt" value={formatInteger(summary.attempt)} />
        <Metric label="Waiting" value={summary.waiting ? "yes" : "no"} />
      </section>

      <div className="workspace-grid">
        <OrdersTable result={orders} />
        <DeskMetrics result={grouped} />
      </div>
    </main>
  );
}

function OrdersTable(props: { readonly result: LiveQueryResult<OrderWindowRow> }) {
  return AsyncResult.match(props.result, {
    onInitial: () => <PanelState title="Open order window" message="Connecting to /rpc" />,
    onFailure: () => <PanelState title="Open order window" message="Subscription failed" />,
    onSuccess: ({ value }) => (
      <section className="panel orders-panel" data-waiting={String(props.result.waiting)}>
        <div className="panel__header">
          <div>
            <h2>Open order window</h2>
            <p>{value.status}</p>
          </div>
          <strong>{formatInteger(value.totalRows)} total</strong>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Order</th>
                <th>Symbol</th>
                <th>Desk</th>
                <th>Status</th>
                <th className="number">Qty</th>
                <th className="number">Price</th>
                <th className="number">Notional</th>
              </tr>
            </thead>
            <tbody>
              {value.rows.map((row) => (
                <tr key={row.id}>
                  <td className="mono">{row.id}</td>
                  <td>{row.symbol}</td>
                  <td>{row.desk}</td>
                  <td>
                    <span className="status-pill" data-order-status={row.status}>
                      {row.status}
                    </span>
                  </td>
                  <td className="number">{formatInteger(row.quantity)}</td>
                  <td className="number">{formatMoney(row.price)}</td>
                  <td className="number strong">{formatMoney(row.notional)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    ),
  });
}

function DeskMetrics(props: { readonly result: LiveQueryResult<DeskMetricRow> }) {
  return AsyncResult.match(props.result, {
    onInitial: () => <PanelState title="Grouped desk metrics" message="Waiting for snapshot" />,
    onFailure: () => <PanelState title="Grouped desk metrics" message="Grouped refresh failed" />,
    onSuccess: ({ value }) => (
      <section className="panel grouped-panel" data-waiting={String(props.result.waiting)}>
        <div className="panel__header">
          <div>
            <h2>Grouped desk metrics</h2>
            <p>{value.status}</p>
          </div>
          <strong>{formatInteger(value.totalRows)} groups</strong>
        </div>
        <div className="metric-list">
          {value.rows.map((row) => (
            <div className="metric-row" key={`${row.desk}_${row.status}`}>
              <div>
                <span className="metric-row__title">
                  {row.desk} / {row.status}
                </span>
                <span>{formatInteger(row.orders)} orders</span>
              </div>
              <div>
                <strong>{formatMoney(row.notional)}</strong>
                <span>{formatInteger(row.quantity)} qty</span>
              </div>
            </div>
          ))}
        </div>
      </section>
    ),
  });
}

function Metric(props: { readonly label: string; readonly value: string }) {
  return (
    <div className="metric">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function PanelState(props: { readonly title: string; readonly message: string }) {
  return (
    <section className="panel panel-state">
      <h2>{props.title}</h2>
      <p>{props.message}</p>
    </section>
  );
}

function liveSummary(result: LiveQueryResult<OrderWindowRow>) {
  return AsyncResult.match(result, {
    onInitial: () => ({
      label: "connecting",
      status: "connecting",
      totalRows: 0,
      visibleRows: 0,
      attempt: 0,
      waiting: true,
    }),
    onFailure: () => ({
      label: "error",
      status: "error",
      totalRows: 0,
      visibleRows: 0,
      attempt: 0,
      waiting: false,
    }),
    onSuccess: ({ value }) => ({
      label: value.status,
      status: value.status,
      totalRows: value.totalRows,
      visibleRows: value.rows.length,
      attempt: value.connection.attempt,
      waiting: result.waiting,
    }),
  });
}

function formatInteger(value: number): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatMoney(value: number): string {
  return new Intl.NumberFormat("en-US", {
    currency: "USD",
    maximumFractionDigits: 0,
    style: "currency",
  }).format(value);
}
