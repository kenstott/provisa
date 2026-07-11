// Copyright (c) 2026 Kenneth Stott
// Canary: 7b2e5f91-3a4d-4c8e-6f0b-9d1a2c8e4b37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable, Source, LiveDeliveryConfig, LiveKafkaConfig, LiveOutputConfig } from "../../types/admin";
import type { PlatformSettings } from "../../api/admin";
import { liveCapability, availableStrategies } from "../../liveCapability";
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
  settings,
}: LiveDeliveryFieldsetProps) {
  const src = sources.find((s) => s.id === editingTable.sourceId);
  const stype = (src?.type ?? "").toLowerCase();
  const isEngineDerived = stype === "trino" || src?.id === "__provisa__";
  // Native CDC needs a CDC-capable source (real table) or, for a
  // materialized view, a CDC-capable materialization store (PostgreSQL
  // LISTEN/NOTIFY, or a Debezium-integrated DB).
  const matStoreScheme = (settings?.materialize?.store_url ?? "")
    .split("://")[0]
    .split("+")[0]
    .toLowerCase();
  const matStoreCdc =
    !!editingTable.materialize &&
    (matStoreScheme === "postgresql" || matStoreScheme === "postgres");
  const nativeCdc =
    matStoreCdc || (!isEngineDerived && liveCapability(stype).cdcAvail);
  // debezium + kafka are always offerable — the operator owns the
  // connector/feed's health. poll is always offerable (it needs a
  // watermark column, set above); native only when CDC-capable. So live
  // delivery is always available.
  const strategies = Array.from(
    new Set([
      "poll",
      ...(nativeCdc ? ["native"] : []),
      "debezium",
      "kafka",
      ...(isEngineDerived ? [] : availableStrategies(stype)),
    ]),
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
  const setKafkaConfig = (patch: Partial<LiveKafkaConfig>) => {
    if (!live) return;
    const base: LiveKafkaConfig = live.kafka ?? {
      topic: "",
      format: "json",
      keyColumn: null,
    };
    setLive({ kafka: { ...base, ...patch } });
  };
  const defaultLive: LiveDeliveryConfig = {
    queryId: `${editingTable.sourceId}.${editingTable.tableName}`,
    watermarkColumn:
      editingTable.watermarkColumn || wmCols[0]?.columnName || "",
    pollInterval: 10,
    strategy: (strategies[0] ?? "poll") as LiveDeliveryConfig["strategy"],
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
              <label>
                Strategy
                <select
                  value={live.strategy}
                  onChange={(e) =>
                    setLive({
                      strategy: e.target
                        .value as LiveDeliveryConfig["strategy"],
                    })
                  }
                >
                  {strategies.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Query ID
                <input
                  readOnly
                  value={`${editingTable.sourceId}.${editingTable.tableName}`}
                  style={{ color: "var(--text-muted)", cursor: "default" }}
                />
              </label>
              {live.strategy === "poll" && (
                <>
                  <label>
                    Watermark column
                    <select
                      value={live.watermarkColumn ?? ""}
                      onChange={(e) =>
                        setLive({ watermarkColumn: e.target.value })
                      }
                    >
                      <option value="">Select…</option>
                      {wmCols.map((c) => (
                        <option key={c.columnName} value={c.columnName}>
                          {c.columnName}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Poll interval (s)
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
                  </label>
                </>
              )}
              {live.strategy === "kafka" && (
                <>
                  <label>
                    Kafka topic
                    <input
                      value={live.kafka?.topic ?? ""}
                      onChange={(e) =>
                        setKafkaConfig({ topic: e.target.value })
                      }
                    />
                  </label>
                  <label>
                    Kafka format
                    <input
                      value={live.kafka?.format ?? "json"}
                      onChange={(e) =>
                        setKafkaConfig({ format: e.target.value })
                      }
                    />
                  </label>
                  <label>
                    Key column
                    <input
                      value={live.kafka?.keyColumn ?? ""}
                      onChange={(e) =>
                        setKafkaConfig({
                          keyColumn: e.target.value || null,
                        })
                      }
                    />
                  </label>
                </>
              )}
              {(live.strategy === "native" ||
                live.strategy === "debezium") && (
                <p
                  style={{
                    margin: 0,
                    fontSize: "0.75rem",
                    color: "var(--text-muted)",
                  }}
                >
                  {live.strategy === "debezium"
                    ? "Debezium transport is configured on the source. No extra per-table config."
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
