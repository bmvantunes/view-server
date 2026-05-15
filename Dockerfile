FROM node:26-bookworm-slim

ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH
ENV NODE_ENV=production
ENV VIEW_SERVER_HOST=0.0.0.0
ENV VIEW_SERVER_PORT=3000

WORKDIR /app

RUN npm install --global corepack@latest \
  && corepack enable \
  && corepack prepare pnpm@11.0.9 --activate

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json vite.config.ts ./
COPY .node-version .nvmrc ./
COPY apps ./apps
COPY packages ./packages

RUN pnpm install --frozen-lockfile --ignore-scripts
RUN pnpm --filter @view-server/core build \
  && pnpm --filter @view-server/react build \
  && pnpm --filter orders-demo build

EXPOSE 3000

CMD ["pnpm", "--filter", "orders-demo", "run", "server"]
