import { createFileRoute } from "@tanstack/react-router";
import { MetricsApp } from "../components/MetricsApp";

export const Route = createFileRoute("/")({ component: App });

function App() {
  return <MetricsApp />;
}
