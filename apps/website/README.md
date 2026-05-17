# Orders Demo

This app is the minimal product loop for View Server:

```text
Effect websocket server -> deterministic publishes -> React useLiveQuery
```

Run the server:

```bash
vp run orders-demo#server
```

Run the browser:

```bash
vp run orders-demo#dev
```

Run the browser-mode contract smoke:

```bash
vp run orders-demo#test
```

Environment knobs:

- `VIEW_SERVER_PORT=3000`
- `VIEW_SERVER_HOST=127.0.0.1`
- `VIEW_SERVER_DEMO_ROWS=800`
- `VIEW_SERVER_DEMO_PUBLISH_INTERVAL_MS=350`
- `VITE_VIEW_SERVER_RPC_URL=ws://127.0.0.1:3000/rpc`

The demo intentionally uses the same public APIs a product app should use:

- `defineConfig` and Effect Schema in `src/view-server.ts`
- `layerViewServerWebsocketServer` in `src/server.ts`
- `createViewServerReact(config)` and `useLiveQuery` in `src/App.tsx`
- `AsyncResult.match` for connecting/live/stale/error rendering
