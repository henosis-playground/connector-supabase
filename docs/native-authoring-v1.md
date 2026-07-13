# Native Supabase authoring v1

The authoring source is an ordinary Supabase CLI project. Authors continue to run `supabase init`,
`supabase migration new`, `supabase db reset`, `supabase db diff`, and the rest of the native
workflow. Henosis reads those files in place; it does not copy them into a TypeScript component or
ask authors to maintain a second migration manifest.

```text
repository/
├── henosis.toml
└── supabase/
    ├── config.toml
    ├── migrations/
    │   └── <timestamp>_<name>.sql
    └── seed.sql                 # optional; native reset/seed behavior, not reconciled by v1
```

`supabase/config.toml` and `supabase/migrations` are the authoritative connector input:

- `project_id` becomes `ComponentSpec.name`.
- `[api].enabled` and `[api].schemas` determine whether the owned schema is exposed. There is no
  Henosis exposure flag that could drift from the native list.
- every `.sql` file directly under `supabase/migrations` is sorted by filename. Its stem is the
  migration ID, its exact UTF-8 contents are the SQL, and its checksum is computed from those exact
  bytes. There is no authored migration list or checksum copy.
- the native pair `GRANT USAGE ON SCHEMA <schema> TO anon` and
  `GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO anon` derives the connector's `read` policy.
  Both statements or neither must exist, so the connector-managed convergence check cannot silently
  disagree with native SQL.

Supabase seed files remain native sources for `supabase start`, `supabase db reset`, and explicit
`supabase db push --include-seed`. The v1 reconciler deliberately does not deploy seed data: seeds
are repeatable data loading, while its approval policy currently permits only additive schema
transitions. `[db.migrations].schema_paths`, Edge Functions, Auth, Storage, Realtime, and other
`config.toml` sections also remain valid Supabase configuration but are outside this backend's v1
reconciliation scope. The derivation leaves them untouched.

## Minimal Henosis marker

The only added file is `henosis.toml` at the repository root:

```toml
api_version = "henosis.dev/supabase-component/v1"
schema = "catalog"
depends_on = []
```

- `api_version` is the marker: it says this repository produces a Supabase Henosis component and
  pins the derivation contract.
- `schema` establishes component ownership, which Supabase's project-wide configuration does not
  express. It also becomes `resourceId`; v1 does not ask for two names for one identity.
- `depends_on` is graph metadata Supabase cannot express. Each entry is a resolved 32-byte
  component-spec hash encoded as 64 lowercase hexadecimal characters. It maps directly to
  `ComponentSpec.depends_on`. Empty components keep the native-looking `[]`.

The connector assignment is implied by `api_version`, not repeated. The output schema is a fixed
contract supplied by this integration for `project`, `database`, `schema`, `apiUrl`, `restUrl`,
`databaseUrlRef`, and `anonKeyRef`; authors cannot make it diverge from what the backend publishes.
The local backend target (`local` / `henosis-local` / `postgres`) is deployment configuration and is
also supplied by the integration rather than copied into the component repository.

The marker is strict: unknown fields and versions fail. The derivation also requires native
migrations to remain enabled and reuses the connector's own identity/schema boundary validator.

## Derivation

From this repository:

```console
cargo run -p henosis-supabase-derive -- /path/to/component-repository
```

The command prints deterministic component-spec material as JSON. Integrations can call the
`henosis-supabase-authoring::derive_component` library directly and submit its `name`, `connector`,
`outputs_schema`, `depends_on`, and exact serialized `connector_context` bytes to core.

Derivation intentionally does not run Supabase or connect to a database. Backend plan validation
still owns target-aware checks and reports native migration policy errors, such as destructive SQL,
through normal Henosis plan diagnostics.
