# Supabase review plan v1

Every target mutation is proposed by an immutable executable plan and a redacted review projection.
Both are generated from one domain plan. The review projection carries the executable SHA-256
digest, so an apply can never silently substitute a refreshed proposal.

The machine projection is canonical JSON with this v1 shape:

```json
{
  "apiVersion": "henosis.dev/supabase-review-plan/v1",
  "planId": "sha256:...",
  "executableDigest": "sha256:...",
  "connector": "supabase",
  "connectorBuild": "0.0.0",
  "policyVersion": "supabase-local-v1",
  "graphId": "...",
  "generation": "1",
  "sliceSequence": "7",
  "target": "local/henosis-local/postgres",
  "desiredDigest": "blake3:...",
  "observedDigest": "blake3:...",
  "journalTail": "19",
  "operations": [],
  "plannedOutputs": []
}
```

Each operation has a stable ID and logical address, component scopes, execution class
(`reconciled-object` or `ordered-transition`), action, redacted before/after values, destructive
level, dependencies, preconditions, and (for migrations) immutable ID/checksum. Sensitive values
are absent, not masked. Database credentials and API keys can appear only as reference URIs in
planned outputs.

The Markdown projection renders the same identity and operations for a human. Both representations
are written atomically under `HENOSIS_STATE_DIR/reviews/<plan-id>/`. The private executable payload
is stored separately under `plans/`; the S2 journal retains plan/operation identities and receipts,
not the executable SQL body.

## Freshness and apply

A plan binds all of:

- graph ID, desired generation, and exact materialized slice sequence;
- connector build and policy version;
- target identity and desired digest;
- the observed target digest;
- the S2 operation-journal tail immediately after `PlanCreated` is appended.

The next pass loads that exact payload. Before mutation it verifies the journal fence, opens a
serializable PostgreSQL transaction, takes the target advisory lock, and re-observes the target in
the transaction. A mismatch yields `supabase.plan.stale`; nothing from that plan is applied. The
connector then plans again.

One reconcile pass applies at most the first operation from the reviewed plan. After a successful
receipt it requeues and plans the remaining level from fresh target truth. A plan is a state-relative
proposal, never a claim that its operation will succeed.
