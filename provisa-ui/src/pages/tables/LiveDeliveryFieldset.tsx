// Copyright (c) 2026 Kenneth Stott
// Canary: 7b2e5f91-3a4d-4c8e-6f0b-9d1a2c8e4b37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable, Source, LiveDeliveryConfig, LiveOutputConfig } from "../../types/admin";
import type { PlatformSettings } from "../../api/admin";
import { isWatermarkEligible } from "./helpers";

interface LiveDeliveryFieldsetProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  editingColumnTypes: Record<string, string>;
  sources: Source[];
  settings: PlatformSettings | null;
}

export function LiveDeliveryFieldset({
  editingTable,
  setEditingTable,
  editingColumnTypes,
  sources,
}: LiveDeliveryFieldsetProps) {
  const src = sources.find((s) => s.id === editingTable.sourceId);
  const stype = (src?.type ?? "").toLowerCase();
  const isEngineDerived = stype === "trino" || src?.id === "__provisa__";
  // Live Delivery is the OUTBOUND axis. The mechanism — repeat a push stream
  // vs. append/replace poll — is DERIVED from the effective Change Signal
  // (the inbound axis), never re-chosen here (REQ-932: change_signal subsumes
  // the legacy live.strategy). Live Delivery only owns the outbound choices:
  // on/off, watermark selection (append vs replace), poll interval, outputs.
  const effectiveSignal =
    editingTable.changeSignal || src?.changeSignal || "ttl";
  const isPushSignal = ["native", "debezium", "kafka"].includes(
    effectiveSignal,
  );
  const live = editingTable.live;
  const setLive = (patch: Partial<LiveDeliveryConfig>) =>
    setEditingTable(
      live
        ? { ...editingTable, live: { ...live, ...patch } }
        : editingTable,
    );
  const wmCols = editingTable.columns.filter((c) => {
    const dt = editingColumnTypes[c.columnName];
    return !dt || isWatermarkEligible(dt);
  });
  // Append vs replace is derived, not chosen (mirrors change_signal.select_landing_shape):
  // a poll signal WITH a watermark appends past MAX(watermark); WITHOUT one it
  // full-replaces and the output content-hash suppresses unchanged ripples.
  const effWatermark = live?.watermarkColumn || "";
  const pollMode: "append" | "replace" | null = isPushSignal
    ? null
    : effWatermark
      ? "append"
      : "replace";
  const refreshInterval = editingTable.materialize
    ? editingTable.mvRefreshInterval
    : null;
  const kafkaOut =
    live?.outputs.find((o) => o.type === "kafka") ?? null;
  const setKafkaOut = (patch: Partial<LiveOutputConfig>) => {
    if (!live) return;
    const others = live.outputs.filter((o) => o.type !== "kafka");
    const base: LiveOutputConfig = kafkaOut ?? {
      type: "kafka",
      topic: "",
      keyColumn: null,
      bootstrapServers: "",
    };
    setLive({ outputs: [...others, { ...base, ...patch }] });
  };
  const defaultLive: LiveDeliveryConfig = {
    queryId: `${editingTable.sourceId}.${editingTable.tableName}`,
    watermarkColumn: isPushSignal
      ? ""
      : editingTable.watermarkColumn || wmCols[0]?.columnName || "",
    pollInterval: 10,
    // Derived from the effective Change Signal; kept only for the REQ-932
    // legacy read-through in reconcile (change_signal is authoritative).
    strategy: (isPushSignal
      ? effectiveSignal
      : "poll") as LiveDeliveryConfig["strategy"],
    kafka: null,
    outputs: [],
  };
  return (
    <fieldset
      className="live-delivery"
      style={{
        gridColumn: "1 / -1",
        border: "1px solid var(--border)",
        borderRadius: "0.4rem",
        padding: "0.5rem 0.75rem",
      }}
    >
      <legend style={{ fontSize: "0.8rem", fontWeight: 600 }}>
        Live Delivery{live ? " — active" : ""}
      </legend>
      {(
        <>
          {isEngineDerived && !editingTable.materialize && (
            <p
              style={{
                margin: "0 0 0.4rem",
                fontSize: "0.72rem",
                color: "var(--warn, #d99)",
              }}
            >
              Each poll recomputes this view. For frequent polling, check{" "}
              <em>Materialized View</em> so polls read the stored table.
            </p>
          )}
          <label
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.4rem",
              fontWeight: "normal",
            }}
          >
            <input
              type="checkbox"
              checked={!!live}
              onChange={(e) =>
                setEditingTable({
                  ...editingTable,
                  live: e.target.checked ? defaultLive : null,
                })
              }
              style={{ width: "auto" }}
            />
            Enable live delivery
          </label>
          {live && (
            <div
              style={{
                display: "grid",
                gap: "0.5rem",
                marginTop: "0.5rem",
              }}
            >
              <div
                data-testid="live-mechanism"
                style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}
              >
                {isPushSignal ? (
                  <>
                    Repeats the <strong>{effectiveSignal}</strong> change
                    stream to subscribers. Inbound detection is set by{" "}
                    <em>Change Signal</em> above — no watermark or poll interval
                    here.
                  </>
                ) : pollMode === "append" ? (
                  <>
                    Append poll on <strong>{effWatermark}</strong> — each
                    interval fetches rows past MAX(watermark).
                  </>
                ) : (
                  <>
                    Full-replace poll — no watermark column, so each poll
                    re-scans and the output content-hash suppresses unchanged
                    results. Suits small or non-monotonic tables.
                  </>
                )}
              </div>
              <label>
                Query ID
                <input
                  readOnly
                  value={`${editingTable.sourceId}.${editingTable.tableName}`}
                  style={{ color: "var(--text-muted)", cursor: "default" }}
                />
              </label>
              {!isPushSignal && (
                <>
                  {wmCols.length > 0 && (
                    <label>
                      <span
                        title="Pick the monotonic change column to append rows past MAX(watermark). Leave as 'None' to full-replace each poll — the output content-hash suppresses unchanged results."
                      >
                        Watermark column
                      </span>
                      <select
                        value={live.watermarkColumn ?? ""}
                        onChange={(e) =>
                          setLive({ watermarkColumn: e.target.value })
                        }
                      >
                        <option value="">None → full replace</option>
                        {wmCols.map((c) => (
                          <option key={c.columnName} value={c.columnName}>
                            {c.columnName}
                          </option>
                        ))}
                      </select>
                    </label>
                  )}
                  <label>
                    <span
                      title="How often the live stream re-checks the source and pushes changes to subscribers (SSE/Kafka sink). Independent of the MV Refresh Interval (which rebuilds the stored table for queries) and Cache TTL (which caches query results)."
                    >
                      Poll interval (s)
                    </span>
                    <input
                      type="number"
                      min={1}
                      value={live.pollInterval}
                      onChange={(e) =>
                        setLive({
                          pollInterval: Number(e.target.value) || 10,
                        })
                      }
                    />
                    {refreshInterval != null &&
                      live.pollInterval < refreshInterval && (
                        <span
                          style={{
                            fontSize: "0.72rem",
                            color: "var(--warn, #d99)",
                          }}
                        >
                          Polling faster than the {refreshInterval}s refresh
                          interval re-reads unchanged data — the materialized
                          table only changes on refresh.
                        </span>
                      )}
                  </label>
                </>
              )}
              {isPushSignal && (
                <p
                  style={{
                    margin: 0,
                    fontSize: "0.75rem",
                    color: "var(--text-muted)",
                  }}
                >
                  {effectiveSignal === "debezium"
                    ? "Debezium transport is configured on the source. No extra per-table config."
                    : effectiveSignal === "kafka"
                      ? "Kafka feed is configured on the source. No extra per-table config."
                      : "Native change stream requires no extra per-table config."}
                </p>
              )}
              <div>
                <div
                  style={{
                    fontSize: "0.75rem",
                    fontWeight: 600,
                    marginBottom: "0.25rem",
                  }}
                >
                  Outputs
                </div>
                <label
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "0.4rem",
                    fontWeight: "normal",
                  }}
                >
                  <input
                    type="checkbox"
                    checked
                    readOnly
                    disabled
                    style={{ width: "auto" }}
                  />
                  SSE fanout (always on)
                </label>
                <label
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "0.4rem",
                    fontWeight: "normal",
                  }}
                >
                  <input
                    type="checkbox"
                    checked={!!kafkaOut}
                    onChange={(e) => {
                      if (e.target.checked) setKafkaOut({});
                      else
                        setLive({
                          outputs: live.outputs.filter(
                            (o) => o.type !== "kafka",
                          ),
                        });
                    }}
                    style={{ width: "auto" }}
                  />
                  Kafka sink
                </label>
                {kafkaOut && (
                  <div
                    style={{
                      display: "grid",
                      gap: "0.4rem",
                      marginTop: "0.35rem",
                      paddingLeft: "1.4rem",
                    }}
                  >
                    <label>
                      Bootstrap servers
                      <input
                        value={kafkaOut.bootstrapServers ?? ""}
                        placeholder="kafka:9092"
                        onChange={(e) =>
                          setKafkaOut({
                            bootstrapServers: e.target.value,
                          })
                        }
                      />
                    </label>
                    <label>
                      Topic
                      <input
                        value={kafkaOut.topic ?? ""}
                        onChange={(e) =>
                          setKafkaOut({ topic: e.target.value })
                        }
                      />
                    </label>
                    <label>
                      Key column
                      <input
                        value={kafkaOut.keyColumn ?? ""}
                        onChange={(e) =>
                          setKafkaOut({
                            keyColumn: e.target.value || null,
                          })
                        }
                      />
                    </label>
                  </div>
                )}
              </div>
            </div>
          )}
        </>
      )}
    </fieldset>
  );
}
