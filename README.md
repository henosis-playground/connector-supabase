# connector-supabase

The Henosis reconciler for a local self-hosted Supabase project.

It implements the generated `henosis.v1.ConnectorService` from core's
`fix/verification-loop-1` branch. Each complete graph slice is strictly accepted, planned against
fresh PostgreSQL/PostgREST truth, rendered into machine and Markdown review projections, and
reconciled one exact operation per pass. The connector reports one atomic complete slice level and
publishes outputs only after re-observation proves convergence.

The preferred authoring contract is the ordinary Supabase CLI project itself, with no Henosis marker
or sidecar, as specified in [docs/native-authoring-v1.md](docs/native-authoring-v1.md). Its derivation
library and `henosis-supabase-derive` tool produce the strict backend bytes documented in
[docs/component-context-v1.md](docs/component-context-v1.md). Plan freshness and projection
semantics are [docs/review-plan-v1.md](docs/review-plan-v1.md). Durable minimal state is
[docs/operation-journal-v1.md](docs/operation-journal-v1.md).

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

## Native authoring

In a component repository initialized with `supabase init`, keep `supabase/config.toml`, native
timestamped migrations, and optional seeds in their normal locations. The `supabase/` directory is
the complete authoring contract: `project_id` supplies the component name and derives its owned
schema, while migration input comments can declare upstream slots without SQL text substitution.
Inspect the exact derived component material with:

```console
cargo run -p henosis-supabase-derive -- /path/to/repository
```

## Service configuration

| Variable | Default | Purpose |
|---|---|---|
| `HENOSIS_BIND` | `0.0.0.0:8082` | ConnectRPC listen address |
| `HENOSIS_CORE_URL` | `http://core:8080` | Core callback/recovery origin |
| `HENOSIS_CORE_TOKEN` | unset | Optional core bearer token |
| `HENOSIS_STATE_DIR` | `/var/lib/henosis-connector-supabase/state` | Checkpoints, private plans, and review projections |
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
