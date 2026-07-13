# connector-supabase

The Henosis reconciler for a local self-hosted Supabase project.

It implements only Supabase target lifecycle hooks on `connector-sdk`: strict target decoding,
fresh PostgreSQL/PostgREST observation, immutable planning, one exact operation per apply pass, and
conservative retirement. The SDK is the sole core-contract dependency and owns push serving,
durable slice checkpoints, plan/review persistence, report construction and delivery, publication
identity, keyed scheduling, retries, and common telemetry. Outputs are published only after fresh
observation proves convergence.

The authoring contract is [docs/component-context-v1.md](docs/component-context-v1.md). Plan
freshness and projection semantics are [docs/review-plan-v1.md](docs/review-plan-v1.md). Durable
minimal state is [docs/operation-journal-v1.md](docs/operation-journal-v1.md).

## V1 scope

Self-hosted Supabase is one project and does not expose the hosted platform's project-management
API. V1 therefore reconciles owned PostgreSQL schemas, additive ordered migrations, anonymous read
grants, and PostgREST exposed-schema configuration inside the pre-provisioned `henosis-local`
project. It deliberately does not fake project/database creation, mirror PostgreSQL into connector
state, auto-apply destructive SQL, or place credentials in world outputs.

The additive `phase-f` Compose profile under `/home/null/Work/henosis/infra` runs pinned official
Supabase PostgreSQL, PostgREST, and Kong images plus this connector. Generate its ignored local
Docker secrets once with `bash infra/supabase/generate-dev-secrets.sh`, then start it with
`docker compose --profile phase-f up -d --wait supabase-db supabase-rest supabase-kong connector-supabase`.

## Service configuration

| Variable | Default | Purpose |
|---|---|---|
| `HENOSIS_BIND` | `0.0.0.0:8082` | ConnectRPC listen address |
| `HENOSIS_CORE_URL` | `http://core:8080` | Core callback/recovery origin |
| `HENOSIS_CORE_TOKEN` | unset | Optional core bearer token |
| `HENOSIS_STATE_DIR` | `/var/lib/henosis-connector-supabase/state-sdk-v1` | SDK checkpoints, private plans, and review projections |
| `S2_*` | required | Existing basin coordinates and token |
| `HENOSIS_SUPABASE_JOURNAL_STREAM` | `connector-supabase-local-v1` | Target operation-journal stream |
| `HENOSIS_SUPABASE_HOST` | `supabase-db` | Target PostgreSQL host |
| `HENOSIS_SUPABASE_PORT` | `5432` | Target PostgreSQL port |
| `HENOSIS_SUPABASE_PASSWORD_FILE` | `/run/secrets/supabase-postgres-password` | Trusted password secret |
| `HENOSIS_SUPABASE_API_URL` | `http://127.0.0.1:4484` | Credential-free public output origin |

## Layout

<!-- LINT.IfChange(layout_rules) -->
- `crates/` — library crates and reusable supporting crates
- `tests/` — workspace member crates that build integration and end-to-end test binaries
- `crates/workspace-hack/` — cargo-hakari dependency unification (auto-generated, do not edit)
- Reusable test harnesses, fixtures, and helpers belong in `crates/`, not `tests/`
- Other binary categories should live in their own top-level directories, such as `services/` or `tools/`
<!-- LINT.ThenChange(//AGENTS.md:layout_rules) -->

## Commands

Use `just` to discover and run common tasks:

<!-- LINT.IfChange(command_recipes) -->
- `just lint` — run all lints (fmt, clippy, deny, pre-commit). Always run after making changes.
- `just test` — run all tests with optimized third-party dependencies
- `just doc` — build docs
<!-- LINT.ThenChange(//AGENTS.md:command_recipes) -->
