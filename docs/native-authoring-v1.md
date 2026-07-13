# Native Supabase authoring v1

The authoring source is an ordinary Supabase CLI project. Henosis reads the native files in place;
there is no marker, manifest, or copied component definition.

```text
repository/
└── supabase/
    ├── config.toml
    ├── migrations/
    │   └── <timestamp>_<name>.sql
    └── seed.sql                 # optional; native reset/seed behavior, not reconciled by v1
```

The existence of `supabase/config.toml` selects the Supabase connector. `project_id` becomes the
component name. If it is empty, derivation uses the repository directory name, matching Supabase's
own default. The owned PostgreSQL schema and stable `resourceId` are the project name with hyphens
normalized to underscores; for example, `service-d` owns `service_d`.

`supabase/config.toml` and `supabase/migrations` are authoritative:

- `[api].enabled` and `[api].schemas` determine whether the derived owned schema is exposed.
- `[db.migrations].enabled` must remain true.
- every `.sql` file directly under `supabase/migrations` is sorted by filename. Its stem is the
  migration ID, its exact UTF-8 contents are the SQL, and its checksum is computed from those bytes.
- matching native `GRANT USAGE ON SCHEMA <schema> TO anon` and
  `GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO anon` statements derive anonymous read access.
  Both or neither must be present.
- connector assignment, output schema, and the local `local/henosis-local/postgres` target are
  integration conventions, not author-maintained metadata.

Seeds, declarative schema paths, Edge Functions, Auth, Storage, Realtime, and unrelated native
configuration remain valid Supabase input but are outside this backend's v1 reconciliation scope.

## Upstream input slots

A migration may declare current-generation upstream values in SQL comments:

```sql
-- henosis:input catalog_url=${henosis:0101010101010101010101010101010101010101010101010101010101010101.apiUrl}
-- henosis:input region=${henosis:8989898989898989898989898989898989898989898989898989898989898989.region:-"local"}

create table service_d.settings (
  catalog_url text not null default current_setting('henosis.input.catalog_url'),
  region text not null default current_setting('henosis.input.region')
);
```

The deployed core contract identifies upstream publications by immutable component-spec hash, not by
component name, so the producer segment is exactly 64 lowercase hexadecimal characters. The final
segment is a top-level output property. `:-<json>` declares an optional JSON default; without it the
slot is required. Slot names and output properties use lowercase letters, digits, and underscores,
beginning with a letter.

Henosis never substitutes text into SQL. At plan materialization it resolves each slot and carries the
bound value in the private executable plan. Immediately before running that migration, the connector
sets a transaction-local PostgreSQL setting named `henosis.input.<slot>`. SQL reads it with
`current_setting`. Strings are exposed directly; other JSON values use canonical JSON text. This
keeps quoting and statement structure under the migration author's control.

A missing required value blocks the plan and reports `supabase.input.unbound` with the producer hash
and output name. A default allows reconciliation and produces an informational
`supabase.input.defaulted` diagnostic. Producer hashes referenced by slots are also emitted as the
derived component's `depends_on` values, so dependency metadata cannot drift from actual consumption.

## Derivation

```console
cargo run -p henosis-supabase-derive -- /path/to/component-repository
```

The command prints deterministic component-spec material as JSON. Integrations can call
`henosis-supabase-authoring::derive_component` and submit its `name`, `connector`, fixed
`outputs_schema`, slot-derived `depends_on`, and exact serialized `connector_context` bytes to core.
Derivation is mutation-free: it does not run Supabase or connect to a database.
