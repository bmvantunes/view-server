import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App.tsx";
import "./style.css";

const rootElement = document.getElementById("app");

if (rootElement === null) {
  throw new Error("Missing #app root");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
