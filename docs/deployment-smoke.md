# Deployment Smoke

This smoke proves the repo can build a deployable demo server container and serve the real production-facing surfaces:

- HTTP `/ready`
- HTTP `/health`
- Effect RPC websocket at `/rpc`
- raw and grouped query paths
- publish, deltaPublish, deleteById
- clean shutdown through Docker Compose

It does not add Kafka or chDB. The demo server uses the in-memory snapshot backend so the container smoke stays deterministic and does not require external services.

## Files

- `Dockerfile`
- `docker-compose.production-smoke.yml`
- `scripts/deployment-smoke.sh`
- `apps/website/src/deployment-smoke-client.ts`

## Build Only

```bash
docker compose -f docker-compose.production-smoke.yml build
```

Expected result: Docker builds with Node 26, installs the official Corepack package, prepares pnpm 11.0.9 through `corepack prepare`, installs with `pnpm install --frozen-lockfile --ignore-scripts`, builds `@view-server/core`, builds `@view-server/react`, and builds `orders-demo`.

Lifecycle scripts are disabled because this smoke intentionally uses the memory-only demo server and should not build optional native integrations such as chDB. A production chDB image should install the chDB system/build requirements explicitly and omit `--ignore-scripts`.

## Run The Smoke

```bash
pnpm run smoke:deployment
```

The script:

1. runs `docker compose -f docker-compose.production-smoke.yml up --build -d`
2. waits for `http://127.0.0.1:3100/ready`
3. connects to `ws://127.0.0.1:3100/rpc`
4. queries raw orders
5. queries grouped orders
6. subscribes and receives a snapshot
7. publishes, delta-publishes, and deletes a smoke row
8. verifies health has no leaked subscribers
9. runs `docker compose down --remove-orphans`

Expected output includes an Effect log similar to:

```text
deployment smoke passed rows=<count> version=<version>
```

and the command exits 0.

If the smoke fails, the script prints app container logs before shutting the compose project down.

## Configuration

Host-side env vars:

```text
VIEW_SERVER_DEPLOYMENT_SMOKE_PORT=3100
VS_DEPLOYMENT_SMOKE_PROJECT=view-server-production-smoke
VS_DEPLOYMENT_SMOKE_HTTP_URL=http://127.0.0.1:3100
VS_DEPLOYMENT_SMOKE_RPC_URL=ws://127.0.0.1:3100/rpc
```

Container env vars:

```text
VIEW_SERVER_HOST=0.0.0.0
VIEW_SERVER_PORT=3000
VIEW_SERVER_DEMO_ROWS=128
VIEW_SERVER_DEMO_PUBLISH_INTERVAL_MS=150
```

The compose healthcheck runs inside the container with Node's built-in `fetch`:

```text
http://127.0.0.1:3000/ready
```

## Manual Inspection

Start without the smoke client:

```bash
docker compose -f docker-compose.production-smoke.yml up --build
```

Then inspect:

```bash
node -e "fetch('http://127.0.0.1:3100/ready').then(async (r) => { process.stdout.write(String(r.status)); process.stdout.write('\\n'); process.stdout.write(await r.text()); })"
node -e "fetch('http://127.0.0.1:3100/health').then(async (r) => { process.stdout.write(String(r.status)); process.stdout.write('\\n'); process.stdout.write(await r.text()); })"
```

Shut down:

```bash
docker compose -f docker-compose.production-smoke.yml down --remove-orphans
```

## Known Limitations

- This is a deployment artifact smoke, not a production topology test.
- Kafka and chDB are intentionally absent here. Production Kafka/chDB behavior is covered by runtime tests, fault-injection tests, and the production wiring docs.
- Dependency lifecycle scripts are disabled in this image to avoid optional native integration builds in the memory-only smoke.
- The image is optimized for smoke confidence, not final image size. It keeps the workspace install and source tree so the demo server can run through Node 26's TypeScript stripping path.
- Docker must be available locally and able to pull the Node 26 base image.
