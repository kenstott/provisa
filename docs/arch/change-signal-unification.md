# Change Signal Unification

Status: accepted. Tracked under REQ-932 (see `docs/arch/requirements.md`).

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

## Rewire (existing sites)

1. MV/landing build sets `freshness_mode` from `change_signal`. Five `MVDefinition(...)`
   sites omit it today (`api/app.py:950,1004,1049,1883`, `api/admin/schema.py:1476`) so it
   always defaults `ttl`. Pass `to_freshness_mode(resolve(...))` and thread `probe_query`.
2. Provider dispatch keys off `change_signal`. `api/data/subscribe.py:_resolve_provider_type`
   reads `live.strategy`; switch to `to_provider(resolve(...))`.
3. Poll-job reconcile keys off `change_signal` + `Table.watermark_column`.
   `live/reconcile.py:_live_is_poll` builds a poll spec when `is_poll(sig)` and a watermark
   exists, replacing `live.strategy == "poll"`.
4. Validation moves to `change_signal`. `core/config_loader.py:_validate_table_live_delivery`
   capability-gates `change_signal` (debezium/kafka require source `cdc`; push signals require
   a CDC-capable source or materialization store).

## Delete / deprecate

- `live.strategy` and `live.watermark_column` (`core/models.py:424-425`, `api/admin/types.py`,
  `api/admin/_live_mappers.py`, `api/admin/_row_mappers.py`) — superseded by `change_signal`
  and `Table.watermark_column`. `live` shrinks to `{enabled, outputs, poll_interval}` (outbound).
- MV `freshness_mode` becomes an internal derived value only.

## New code (missing consumption layer)

5. Landing paths in `mv/refresh.py` today only replace (`DELETE`+`INSERT` / `CTAS`):
   - Append: `INSERT ... WHERE watermark > cursor` when `watermark_column` is set; persist the
     cursor alongside `mv_refresh_log.input_version`.
   - CDC-apply: consume the debezium/kafka provider and upsert/tombstone by primary key into the
     landed table. This is what makes deletes work.

## Migration

V1, no migrations: hard-cut. Derive at read sites and drop `live.strategy`/`live.watermark_column`
from the model and persistence in one pass. Existing JSONB rows carrying `live.strategy` are
ignored by the loader; a one-time `_row_mappers` shim maps `strategy → change_signal` when the
latter is unset, after which the field stops being written.

## Sequencing (each phase leaves the tree green)

1. Derivation module + rewire 1 (freshness_mode) + replace-path test.
2. Rewire 2/3/4 (provider + reconcile + validation) + publish-downstream test.
3. New append + CDC-apply landing + append/delete test.
4. Delete `live.strategy`/`live.watermark_column`.

## Tests

- Derivation (unit): `change_signal → freshness_mode / provider` for six values plus inherit.
- Publish downstream (integration): a Debezium/Kafka event through the resolved provider reaches
  the SSE stream and a Kafka sink.
- Landing replace (integration): poll signal, no watermark, full rebuild.
- Landing append + delete (integration): watermark signal → incremental insert; `op=d` tombstone
  removes the row.
