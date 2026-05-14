<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

## Review Checklist

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check` and `vp test` to format, lint, type check and test changes.
- [ ] Check if there are `vite.config.ts` tasks or `package.json` scripts necessary for validation, run via `vp run <script>`.

<!--VITE PLUS END-->

## Project Conventions

- Read `/Users/bruno/projects/view-server/plan.md` first and treat it as the source of truth for architecture and priority.
- Use Effect v4 beta patterns throughout runtime code. Prefer `Effect.fn("view-server.<area>.<operation>")` for meaningful Effect service boundaries and `Effect.withSpan(...)` for local inner blocks.
- Add span annotations for runtime context such as topic, request id, subscription id, worker/backend versions, batch size, row counts, total rows, Kafka partition/offset/lag. Do not create spans per row.
- Do not use `console.*`; use Effect logging/tracing.
- Tests should import from `vite-plus/test` by default. Use `@effect/vitest` for Effect-specific test helpers when needed. Do not import directly from `vitest` unless a tool configuration file specifically requires it.
- Use `expect`. Do not use `node:assert`, `assert`, or `node:test`.
- Browser tests use Vitest browser mode with Playwright. Do not use Testing Library React or happydom.
- Keep normal application/client/React code free of casts. If a boundary truly needs runtime conversion, prefer Effect Schema decode/encode derived from `defineConfig`.
- Before committing, run focused checks/tests for the changed area and scan for casts, `console.*`, node assert/test imports, and direct `vitest` imports.
