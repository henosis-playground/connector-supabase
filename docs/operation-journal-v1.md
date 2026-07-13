# Supabase operation journal v1

The connector owns one S2 stream per configured Supabase target in the existing Henosis basin. For
the compose target the stream is `connector-supabase-local-v1`. JSON envelopes are append-only and
CAS-protected by S2 sequence number.

The journal retains only operational facts that cannot safely be reconstructed:

- stable `resourceId` to `{project, database, schema}` identity bindings and graph ownership;
- the authoritative S2 plan digest on each target-effect operation and its operation ID;
- operation started/succeeded/failed receipts and uncertainty;
- retirement/release facts.

It is not the plan store: explicit plans and review projections live in the SDK's per-graph S2 plan
stream. It does not mirror tables, columns, grants, PostgREST settings, migration SQL, outputs,
passwords, keys, or a full desired slice. Those facts are re-observed or recovered from core. PostgreSQL's
`henosis_connector.migration_receipts` table is the authoritative target-side ledger for ordered
migrations; the S2 receipt explains which reviewed Henosis operation caused each transition.

If a process stops after `OperationStarted`, recovery re-observes the operation's postcondition. A
present schema, matching migration receipt, or exact API configuration completes the receipt. A
missing postcondition retries the same idempotent operation ID. The connector never infers that a
missing local completion record means the remote call did not happen.
