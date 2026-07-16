# Change Signal Unification

Status: implemented (REQ-932, 2026-07). Tracked in `docs/arch/requirements.yaml`.

## Problem

The inbound question "how do we learn a source changed" is encoded in three places
with overlapping value sets:

| Signal | Values | Runtime role |
|---|---|---|
| `change_signal` (Table/Source) | `ttl` `probe` `ttl_probe` `native` `debezium` `kafka` | none (captured, inert) |
| MV `freshness_mode` | `ttl` `probe` `ttl_probe` | refresh gate |
| `live.strategy` | `poll` `native` `debezium` `kafka` | subscription provider |

`change_signal` is the union: `∩ freshness_mode = {ttl, probe, ttl_probe}` (identical),
`∩ live.strategy = {native, debezium, kafka}` (identical). `poll` is `ttl`/`probe` with a
watermark. The same axis is expressed three times, and `change_signal` — the field designed
to be authoritative — is read nowhere.

## Target model

Three orthogonal axes:

| Axis | Field | Decides |
|---|---|---|
| Inbound detection | `change_signal` (resolve `table.change_signal or source.change_signal`) | how we learn the source changed |
| Append vs replace + subscribable | `Table.watermark_column` | set → incremental append + subscribable; unset → full replace |
| Is it landed | `materialize` / reachability / `cache_ttl` | whether a local copy exists |

`freshness_mode` and `live.strategy` derive from `change_signal`; they are no longer
configured independently.

## Derivation (`provisa/core/change_signal.py`)

```
resolve(table, source) -> str          # table.change_signal or source.change_signal
is_poll(sig) = sig in {ttl, probe, ttl_probe}
is_push(sig) = sig in {native, debezium, kafka}

to_freshness_mode(sig) -> str | None   # poll → same value; push → None (event-driven, no gate)
to_provider(sig, source_type) -> str   # native → source_type; debezium → "debezium"; kafka → "kafka"; poll → source_type
```

Push signals skip the freshness gate — their landed copy is updated by applying incoming
events, not by polling.

## Rewired sites

1. MV/landing build derives `freshness_mode` from `change_signal` via
   `to_freshness_mode(resolve(...))` at the `MVDefinition(...)` construction sites
   (`api/app.py`, `api/admin/schema.py`), threading `probe_query`. Previously these omitted
   `freshness_mode` and always defaulted `ttl`.
2. Provider dispatch keys off `change_signal`. `api/data/subscribe.py:_resolve_provider_type`
   resolves via `to_provider(resolve_effective(...))` rather than reading `live.strategy` directly.
3. Poll-job reconcile keys off `change_signal` + `Table.watermark_column`. `live/reconcile.py`
   builds a poll spec when `is_poll(sig)`; a watermark selects append vs replace landing.
4. Validation keys off `change_signal` in `core/config_loader.py`: debezium/kafka require a
   source `cdc` block; push signals require a CDC-capable source or materialization store.

## Delete / deprecate

- `live.strategy` and `live.watermark_column` (`core/models.py`) are superseded by `change_signal`
  and `Table.watermark_column`. They are retained on the model as a legacy read-through: provider
  dispatch (`api/data/subscribe.py`) and poll reconcile (`live/reconcile.py`) go through
  `change_signal.resolve_effective(...)`, which falls back to `signal_from_strategy(live.strategy)`
  only when no `change_signal` is set. `change_signal` is authoritative when present.
- MV `freshness_mode` becomes an internal derived value only.

## Landing (consumption layer, shipped)

`change_signal.select_landing_shape(sig, watermark_column)` maps the signal to a landing shape,
applied by the DB-agnostic Core ops in `federation/materialize_exec.py` (driven by
`federation/store_writer.py`) — not by engine CTAS in `mv/refresh.py`:

- Replace: `DELETE`+`INSERT` — poll signal with no watermark.
- Append: `INSERT ... WHERE watermark > cursor` — poll signal with a `watermark_column` set.
- CDC-apply: consume the debezium/kafka provider and upsert/tombstone by primary key into the
  landed table via the dialect-agnostic `Connection.upsert`. This is what makes landed deletes work.
  (Subscription *delivery* grain remains insert/update only per REQ-928.)

## Migration

V1, no migrations. Derivation happens at the read sites rather than by dropping the fields:
`change_signal` is authoritative, and `live.strategy`/`live.watermark_column` remain on the model
as a legacy fallback read through `resolve_effective` / `signal_from_strategy`. Existing JSONB rows
carrying `live.strategy` keep working — the resolver maps `strategy → change_signal` when the
latter is unset.

## Sequencing (each phase leaves the tree green)

1. Derivation module + rewire 1 (freshness_mode) + replace-path test.
2. Rewire 2/3/4 (provider + reconcile + validation) + publish-downstream test.
3. New append + CDC-apply landing + append/delete test.
4. Delete `live.strategy`/`live.watermark_column`.

## Tests

Coverage is in unit tests (`tests/unit/test_change_signal.py`, `test_materialize_landing.py`,
`test_subscribe_publish.py`); the integration tests named in early planning
(`test_landing_replace` / `append_delete` / `publish_downstream`) were not created.

- Derivation: `change_signal → freshness_mode / provider / landing shape` for six values plus inherit.
- Publish downstream: a Debezium/Kafka event through the resolved provider reaches the SSE stream
  and a Kafka sink.
- Landing replace: poll signal, no watermark, full rebuild.
- Landing append + CDC-apply: watermark signal → incremental insert; `op=d` tombstone removes the row.
