# View Server Metrics

TanStack Start app for the View Server health topic. The app connects to the Effect RPC websocket
endpoint and renders `__view_server_health` through the public React subscription hooks.

## Scripts

```bash
vp run metrics#dev
vp run metrics#test
vp run metrics#build
```

Runtime endpoint configuration:

- `VITE_VIEW_SERVER_RPC_URL`: full websocket URL, for example `ws://127.0.0.1:3000/rpc`
- `VITE_VIEW_SERVER_RPC_PATH`: websocket path on the current host, default `/rpc`
