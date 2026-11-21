// Copyright (c) 2026 Kenneth Stott
// Canary: 11512393-9871-4333-a5e8-19346666ab3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef, type RefObject } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Checkbox,
  Group,
  Modal,
  NumberInput,
  Switch,
  Text,
  TextInput,
  Tabs,
  Title,
} from "@mantine/core";
import { Pause, Play, Maximize2, X } from "lucide-react";
import {
  updateSettings,
  reloadQueryEngineCatalog,
  restartQueryEngine,
  recomputeSchemaClusters,
} from "../../api/admin";
import { SUBSYSTEM_TRACE_KEYS } from "../../api/admin";
import type { PlatformSettings } from "../../api/admin";
import { useAuth } from "../../context/AuthContext";
import { useCapability } from "../../hooks/useCapability";
import { DebugTracePanel } from "./DebugTracePanel";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { FilterInput } from "./FilterInput";
import { Tip } from "../Tip";
import { matchesQuery } from "../../lib/searchItems";

interface ObsTabProps {
  settings: PlatformSettings;
  setSettings: (s: PlatformSettings) => void;
}

interface TraceEntry {
  ts: number;
  trace_id: string;
  span_id: string;
  name: string;
  status: string;
  duration_ms: number | null;
  attrs: Record<string, unknown>;
}

const API_BASE = import.meta.env.VITE_API_BASE || "";

// GET /admin/traces/latest: when the ops traces table last received a row.
interface TracesTableLatest {
  tracked: boolean;
  latest?: string | null;
  age_seconds?: number | null;
  stale_after_seconds: number;
}

const TABLE_CHECK_MS = 30_000;

// What the ops traces table says about itself while an endpoint is set. The read is the server's;
// nothing is inferred from the endpoint's URL. `latest` is null until the first answer, and when
// the table is not read by this engine. `failure` holds why the last check could not be made (the
// status code or the error message); a later successful check clears it.
function useTracesTableLatest(endpointSet: boolean): {
  latest: TracesTableLatest | null;
  failure: string | null;
} {
  const [latest, setLatest] = useState<TracesTableLatest | null>(null);
  const [failure, setFailure] = useState<string | null>(null);

  useEffect(() => {
    if (!endpointSet) return;
    let alive = true;
    const check = async () => {
      try {
        const resp = await fetch(`${API_BASE}/admin/traces/latest`);
        if (!resp.ok) {
          if (alive) setFailure(`HTTP ${resp.status}`);
          return;
        }
        const json: TracesTableLatest = await resp.json();
        if (alive) {
          setLatest(json);
          setFailure(null);
        }
      } catch (err) {
        if (alive) setFailure(err instanceof Error ? err.message : String(err));
      }
    };
    check();
    const id = setInterval(check, TABLE_CHECK_MS);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [endpointSet]);

  return endpointSet ? { latest, failure } : { latest: null, failure: null };
}

function TraceList({
  traces,
  filter,
  selected,
  setSelected,
  listRef,
}: {
  traces: TraceEntry[];
  filter: string;
  selected: TraceEntry | null;
  setSelected: (t: TraceEntry | null) => void;
  listRef?: RefObject<HTMLDivElement | null>;
}) {
  const { t } = useTranslation();
  const statusColor = (s: string) =>
    s === "OK" ? "var(--success)" : s === "ERROR" ? "var(--error, #e55)" : "var(--text-muted)";

  const visible = traces.filter((t) => matchesQuery([t.name, t.status, t.trace_id], filter));

  return (
    <div className="trace-feed-list" ref={listRef}>
      {visible.length === 0 ? (
        <Text className="trace-empty" size="sm" c="dimmed">
          {traces.length === 0
            ? t("observabilityTab.noSpansYet")
            : t("observabilityTab.noSpansMatch")}
        </Text>
      ) : (
        visible.map((tr) => (
          <div
            key={tr.span_id}
            className={`trace-row${selected?.span_id === tr.span_id ? " trace-row--selected" : ""}`}
            onClick={() => setSelected(selected?.span_id === tr.span_id ? null : tr)}
            data-testid={`trace-row-${tr.span_id}`}
          >
            <span className="trace-ts">{new Date(tr.ts * 1000).toLocaleTimeString()}</span>
            <span className="trace-name">{tr.name}</span>
            <span className="trace-status" style={{ color: statusColor(tr.status) }}>
              {tr.status}
            </span>
            <span className="trace-dur">
              {tr.duration_ms != null ? `${tr.duration_ms}ms` : "—"}
            </span>
            {selected?.span_id === tr.span_id && (
              <div className="trace-detail" onClick={(e) => e.stopPropagation()}>
                <div>
                  <strong>{t("observabilityTab.traceLabel")}</strong> <code>{tr.trace_id}</code>
                </div>
                <div>
                  <strong>{t("observabilityTab.spanLabel")}</strong> <code>{tr.span_id}</code>
                </div>
                <div>
                  <strong>{t("observabilityTab.timeLabel")}</strong>{" "}
                  {new Date(tr.ts * 1000).toLocaleTimeString()}
                </div>
                {Object.keys(tr.attrs).length > 0 && (
                  <pre className="trace-attrs">{JSON.stringify(tr.attrs, null, 2)}</pre>
                )}
              </div>
            )}
          </div>
        ))
      )}
    </div>
  );
}

function TraceFeed() {
  const { t } = useTranslation();
  const [traces, setTraces] = useState<TraceEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<TraceEntry | null>(null);
  const [expanded, setExpanded] = useState(false);
  const listRef = useRef<HTMLDivElement>(null);
  const pausedRef = useRef(paused);
  /* eslint-disable-next-line react-hooks/refs --
     latest-value ref mirrors `paused` for the async poll callback (standard ref pattern) */
  pausedRef.current = paused;

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      if (!alive || pausedRef.current) return;
      try {
        const resp = await fetch(`${API_BASE}/admin/traces/recent?limit=50`);
        const json = await resp.json();
        if (alive && !pausedRef.current) setTraces(json.traces ?? []);
      } catch {
        /* ignore */
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const headerContent = (
    <>
      <span style={{ flex: 1 }}>{t("observabilityTab.traces")}</span>
      {/* The list’s filter is the shared one; a click in it must not fold the panel. */}
      <span onClick={(e) => e.stopPropagation()}>
        <FilterInput
          value={filter}
          onChange={setFilter}
          placeholder={t("observabilityTab.filterPlaceholder")}
          testId="trace-filter"
        />
      </span>

      <Group flex={1} justify="flex-end" align="center" gap="0.4rem">
        <Tip text={paused ? t("observabilityTab.resume") : t("observabilityTab.pause")} named>
          <ActionIcon
            className="trace-pause-btn"
            style={{ marginInlineStart: "0.5rem" }}
            onClick={() => setPaused((p) => !p)}
            aria-label={paused ? t("observabilityTab.resume") : t("observabilityTab.pause")}
            variant="subtle"
          >
            {paused ? <Play size={14} /> : <Pause size={14} />}
          </ActionIcon>
        </Tip>
        <Tip text={expanded ? t("observabilityTab.collapse") : t("observabilityTab.expand")} named>
          <ActionIcon
            className="trace-expand-btn"
            onClick={(e) => {
              e.stopPropagation();
              setExpanded((v) => !v);
            }}
            aria-label={expanded ? t("observabilityTab.collapse") : t("observabilityTab.expand")}
            variant="subtle"
          >
            {expanded ? <X size={14} /> : <Maximize2 size={14} />}
          </ActionIcon>
        </Tip>
      </Group>
    </>
  );

  return (
    <>
      <div className="trace-feed" data-testid="trace-feed">
        <div className="trace-feed-header">{headerContent}</div>
        <TraceList
          traces={traces}
          filter={filter}
          selected={selected}
          setSelected={setSelected}
          listRef={listRef}
        />
      </div>
      <Modal
        opened={expanded}
        onClose={() => setExpanded(false)}
        size="90vw"
        title={t("observabilityTab.traces")}
        transitionProps={{ duration: 0 }}
      >
        <div style={{ height: "80vh", overflow: "auto" }}>
          <TraceList
            traces={traces}
            filter={filter}
            selected={selected}
            setSelected={setSelected}
          />
        </div>
      </Modal>
    </>
  );
}

// REQ-1349: the OTLP exporter block is deployment-wide, so GET /admin/settings omits it for a
// caller without `platform_settings`; the trace feed beside it is that caller's own traffic, which
// is why only this half of the tab is gated on the block being present.
function OtelSettingsSection({
  settings,
  setSettings,
  otel,
}: ObsTabProps & { otel: NonNullable<PlatformSettings["otel"]> }) {
  const { t } = useTranslation();
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  // The last state the server acknowledged, which is what Cancel restores. The switches write
  // straight through to the parent's settings object, so without a copy taken before editing there
  // is nothing to go back to short of reloading the page.
  const [saved, setSaved] = useState(otel);

  const update = (key: keyof typeof otel, value: unknown) =>
    setSettings({ ...settings, otel: { ...otel, [key]: value } });

  const dirty = JSON.stringify(otel) !== JSON.stringify(saved);

  const save = async () => {
    setSaving(true);
    setMsg("");
    try {
      const result = await updateSettings({ otel: otel });
      setSaved(otel);
      setMsg(`Saved: ${result.updated.join(", ")}`);
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : t("observabilityTab.saveFailed"));
    } finally {
      setSaving(false);
    }
  };

  const cancel = () => {
    setSettings({ ...settings, otel: saved });
    setMsg("");
  };

  const active = Boolean(otel.endpoint);
  // The endpoint the process is exporting to is the saved one, not what is being typed.
  const { latest: tableLatest, failure: tableFailure } = useTracesTableLatest(
    Boolean(saved.endpoint),
  );
  // A failed check leaves the last answer stale, so the failure is shown instead of it.
  const tableStale =
    tableFailure === null &&
    tableLatest !== null &&
    tableLatest.tracked &&
    (tableLatest.latest == null ||
      (tableLatest.age_seconds ?? 0) > tableLatest.stale_after_seconds);

  return (
    <div className="settings-section">
      <Text c="dimmed" size="sm" mb="md">
        {t("observabilityTab.statusLabel")}{" "}
        <Text component="strong" c={active ? "var(--success)" : "dimmed"} span>
          {active
            ? t("observabilityTab.statusExporting", { endpoint: otel.endpoint })
            : t("observabilityTab.statusActiveNoCollector")}
        </Text>
      </Text>
      {tableFailure !== null && (
        <Alert color="red" variant="light" mb="md" data-testid="traces-table-unchecked">
          {t("observabilityTab.tracesTableCheckFailed", { reason: tableFailure })}
        </Alert>
      )}
      {tableStale && tableLatest && (
        <Alert color="yellow" variant="light" mb="md" data-testid="traces-table-stale">
          {tableLatest.latest == null
            ? t("observabilityTab.tracesTableEmpty")
            : t("observabilityTab.tracesTableStale", {
                minutes: Math.round(tableLatest.stale_after_seconds / 60),
              })}
        </Alert>
      )}
      {/* Four unrelated groups of fields stacked into one column ran far past the trace feed
          beside them; as tabs each is a screenful and the page ends where the feed does. */}
      <Tabs defaultValue="exporter" keepMounted={false}>
        <Tabs.List mb="md">
          <Tabs.Tab value="exporter">{t("observabilityTab.otelTitle")}</Tabs.Tab>
          <Tabs.Tab value="subsystems">{t("observabilityTab.subsystemsTitle")}</Tabs.Tab>
          <Tabs.Tab value="pipeline">{t("observabilityTab.pipelineTitle")}</Tabs.Tab>
          <Tabs.Tab value="support">{t("observabilityTab.supportTitle")}</Tabs.Tab>
          <Tabs.Tab value="debug" data-testid="observability-debug-trace-tab">
            {t("debugTracePanel.tabTitle")}
          </Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="exporter">
          <FormLayout>
            <FormField help={t("observabilityTab.endpointHelp")} size="long">
              <TextInput
                label={t("observabilityTab.endpointLabel")}
                value={otel.endpoint}
                onChange={(e) => update("endpoint", e.target.value)}
                placeholder="http://otel-collector:4317"
              />
            </FormField>
            <FormField hint={t("observabilityTab.serviceNameHelp")} size="medium">
              <TextInput
                label={t("observabilityTab.serviceNameLabel")}
                value={otel.service_name}
                onChange={(e) => update("service_name", e.target.value)}
                placeholder="provisa"
              />
            </FormField>
            <FormField help={t("observabilityTab.sampleRateHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.sampleRateLabel")}
                min={0}
                max={1}
                step={0.01}
                value={otel.sample_rate}
                onChange={(v) => update("sample_rate", typeof v === "number" ? v : 0)}
              />
            </FormField>
            <FormField hint={t("observabilityTab.logLevelHelp")} size="medium">
              <TextInput
                label={t("observabilityTab.logLevelLabel")}
                value={otel.log_level}
                onChange={(e) => update("log_level", e.target.value)}
                placeholder="WARNING"
              />
            </FormField>
          </FormLayout>
        </Tabs.Panel>

        {/* REQ-1432: which subsystems emit spans. The catalog database is off by default —
            every catalog read is one of its calls, and they bury the query traces. */}
        <Tabs.Panel value="subsystems">
          <Text c="dimmed" size="sm" mb="md">
            {t("observabilityTab.subsystemsIntro")}
          </Text>
          <FormLayout>
            <FormChecks>
              {SUBSYSTEM_TRACE_KEYS.map((key) => (
                <FormField help={t(`observabilityTab.subsystemHelp.${key}`)} size="short" key={key}>
                  <Switch
                    data-testid={`subsystem-trace-${key}`}
                    label={t(`observabilityTab.subsystem.${key}`)}
                    checked={otel.subsystem_traces[key]}
                    onChange={(e) =>
                      update("subsystem_traces", {
                        ...otel.subsystem_traces,
                        [key]: e.currentTarget.checked,
                      })
                    }
                  />
                </FormField>
              ))}
            </FormChecks>
          </FormLayout>
        </Tabs.Panel>

        <Tabs.Panel value="pipeline">
          <Text c="dimmed" size="sm" mb="md">
            {t("observabilityTab.pipelineIntro")}
          </Text>
          <FormLayout>
            <FormField hint={t("observabilityTab.s3EndpointHelp")} size="long">
              <TextInput
                label={t("observabilityTab.s3EndpointLabel")}
                value={otel.s3_endpoint}
                onChange={(e) => update("s3_endpoint", e.target.value)}
                placeholder="http://minio:9000"
              />
            </FormField>
            <FormField hint={t("observabilityTab.compactCronHelp")} size="medium">
              <TextInput
                label={t("observabilityTab.compactCronLabel")}
                value={otel.compact_cron}
                onChange={(e) => update("compact_cron", e.target.value)}
                placeholder="* * * * *"
              />
            </FormField>
            <FormField help={t("observabilityTab.compactBatchSizeHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.compactBatchSizeLabel")}
                min={1}
                value={otel.compact_batch_size}
                onChange={(v) => update("compact_batch_size", typeof v === "number" ? v : 0)}
              />
            </FormField>
            <FormField help={t("observabilityTab.compactFileChunkHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.compactFileChunkLabel")}
                min={1}
                value={otel.compact_file_chunk}
                onChange={(v) => update("compact_file_chunk", typeof v === "number" ? v : 0)}
              />
            </FormField>
            <FormField help={t("observabilityTab.opsSnapshotRetentionHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.opsSnapshotRetentionLabel")}
                min={0}
                value={otel.ops_snapshot_retention_hours ?? ""}
                onChange={(v) =>
                  update("ops_snapshot_retention_hours", v === "" ? null : Number(v))
                }
              />
            </FormField>
            <FormField help={t("observabilityTab.spanExportDelayHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.spanExportDelayLabel")}
                min={0}
                value={otel.span_export_delay_millis}
                onChange={(v) => update("span_export_delay_millis", typeof v === "number" ? v : 0)}
              />
            </FormField>
            <FormField help={t("observabilityTab.otlp2parquetMaxAgeHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.otlp2parquetMaxAgeLabel")}
                min={0}
                value={otel.otlp2parquet_max_age_secs}
                onChange={(v) => update("otlp2parquet_max_age_secs", typeof v === "number" ? v : 0)}
              />
            </FormField>
            <FormField help={t("observabilityTab.collectorBatchTimeoutHelp")} size="short">
              <NumberInput
                label={t("observabilityTab.collectorBatchTimeoutLabel")}
                min={0}
                value={otel.collector_batch_timeout_ms}
                onChange={(v) =>
                  update("collector_batch_timeout_ms", typeof v === "number" ? v : 0)
                }
              />
            </FormField>
          </FormLayout>
        </Tabs.Panel>

        <Tabs.Panel value="support">
          <Text c="dimmed" size="sm" mb="md">
            {t("observabilityTab.supportIntro")}
          </Text>
          <FormLayout>
            <FormField help={t("observabilityTab.supportEndpointHelp")} size="long">
              <TextInput
                label={t("observabilityTab.supportEndpointLabel")}
                value={otel.support_endpoint}
                onChange={(e) => update("support_endpoint", e.target.value)}
                placeholder="https://otel.provisa.io:4318"
              />
            </FormField>
            <FormChecks>
              <FormField size="long">
                <Checkbox
                  checked={otel.support_redact_sql_literals}
                  onChange={(e) => update("support_redact_sql_literals", e.target.checked)}
                  label={t("observabilityTab.redactSqlLabel")}
                />
              </FormField>
            </FormChecks>
            <FormField hint={t("observabilityTab.redactAttrsHelp")} size="medium">
              <TextInput
                label={t("observabilityTab.redactAttrsLabel")}
                value={(otel.support_redact_attributes ?? []).join(", ")}
                onChange={(e) =>
                  update(
                    "support_redact_attributes",
                    e.target.value
                      .split(",")
                      .map((s) => s.trim())
                      .filter(Boolean),
                  )
                }
                placeholder="user.id, db.user, ..."
              />
            </FormField>
          </FormLayout>
        </Tabs.Panel>

        {/* REQ-1910: debug-trace windows and the per-role hint permission. Its changes apply as
            they are made — the Save below is for the otel settings the other panels edit. */}
        <Tabs.Panel value="debug">
          <DebugTracePanel />
        </Tabs.Panel>
      </Tabs>

      {/* One save for the whole tab: every panel edits the same otel settings object. */}
      <FormFooter
        onSave={save}
        saving={saving}
        onCancel={cancel}
        cancelProps={{ disabled: !dirty }}
        saveProps={{ "data-testid": "observability-save" }}
        style={{ marginTop: "1rem" }}
      >
        {msg && (
          <Text className="upload-msg" size="sm">
            {msg}
          </Text>
        )}
      </FormFooter>
      <Text mt="sm" size="xs" c="dimmed">
        {t("observabilityTab.restartNote")}
      </Text>
    </div>
  );
}

// REQ-1327: the org's page. The caller's own trace feed for every observability holder; beside it
// what the org administers, on the org_settings the routes behind it ask.
export function ObservabilityTab() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const administers = useCapability("org_settings");
  return (
    <div className="observability-layout">
      <TraceFeed />
      {administers && (
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          {activeOrgId !== null && (
            <div className="settings-section">
              <Title order={4} mb="sm">
                {t("debugTracePanel.tabTitle")}
              </Title>
              <DebugTracePanel orgId={activeOrgId} />
            </div>
          )}
          <SchemaClusterActions />
        </div>
      )}
    </div>
  );
}

// REQ-1327: the platform's page, the deployment check on its route: the exporter, org-wide debug
// tracing and the query engine's lifecycle.
export function PlatformObservabilityTab({ settings, setSettings }: ObsTabProps) {
  return (
    <div className="observability-layout">
      {settings.otel && (
        <OtelSettingsSection settings={settings} setSettings={setSettings} otel={settings.otel} />
      )}
      <QueryEngineActions />
    </div>
  );
}

function QueryEngineActions() {
  const { t } = useTranslation();
  const [reloadStatus, setReloadStatus] = useState<string>("");
  const [restartStatus, setRestartStatus] = useState<string>("");
  const [reloading, setReloading] = useState(false);
  const [restarting, setRestarting] = useState(false);

  const handleReload = async () => {
    setReloading(true);
    setReloadStatus("");
    try {
      const result = await reloadQueryEngineCatalog();
      setReloadStatus(
        result.success
          ? t("observabilityTab.catalogReloaded")
          : t("observabilityTab.errorsPrefix", { errors: result.errors.join("; ") }),
      );
    } catch (e: unknown) {
      setReloadStatus(e instanceof Error ? e.message : t("observabilityTab.failed"));
    } finally {
      setReloading(false);
    }
  };

  const handleRestart = async () => {
    setRestarting(true);
    setRestartStatus("");
    try {
      const result = await restartQueryEngine();
      setRestartStatus(t("observabilityTab.restarted", { container: result.container }));
    } catch (e: unknown) {
      setRestartStatus(e instanceof Error ? e.message : t("observabilityTab.failed"));
    } finally {
      setRestarting(false);
    }
  };

  return (
    <div className="settings-section">
      <Title order={4}>{t("observabilityTab.queryEngineTitle")}</Title>
      <Group gap="0.75rem" align="center" wrap="wrap" mt="sm">
        <Button className="btn-primary" onClick={handleReload} disabled={reloading}>
          {reloading ? t("observabilityTab.reloading") : t("observabilityTab.reloadCatalog")}
        </Button>
        <Button
          color="yellow"
          className="btn-warning"
          onClick={handleRestart}
          disabled={restarting}
        >
          {restarting ? t("observabilityTab.restarting") : t("observabilityTab.restartEngine")}
        </Button>
      </Group>
      {reloadStatus && (
        <Text className="upload-msg" size="sm" mt="0.5rem">
          {reloadStatus}
        </Text>
      )}
      {restartStatus && (
        <Text className="upload-msg" size="sm" mt="0.5rem">
          {restartStatus}
        </Text>
      )}
    </div>
  );
}

// REQ-1327: the org's schema clusters, recomputed on the org_settings its route asks.
function SchemaClusterActions() {
  const { t } = useTranslation();
  const [status, setStatus] = useState<string>("");
  const [reclustering, setReclustering] = useState(false);

  const handleRecluster = async () => {
    setReclustering(true);
    setStatus("");
    try {
      const result = await recomputeSchemaClusters();
      setStatus(t("observabilityTab.clustered", { count: result.tables_clustered }));
    } catch (e: unknown) {
      setStatus(e instanceof Error ? e.message : t("observabilityTab.failed"));
    } finally {
      setReclustering(false);
    }
  };

  return (
    <div className="settings-section">
      <Title order={4}>{t("observabilityTab.schemaClustersTitle")}</Title>
      <Group gap="0.75rem" align="center" wrap="wrap" mt="sm">
        <Button
          variant="default"
          className="btn-secondary"
          onClick={handleRecluster}
          disabled={reclustering}
          data-testid="observability-recompute-clusters"
        >
          {reclustering
            ? t("observabilityTab.clustering")
            : t("observabilityTab.recomputeClusters")}
        </Button>
      </Group>
      {status && (
        <Text className="upload-msg" size="sm" mt="0.5rem">
          {status}
        </Text>
      )}
    </div>
  );
}
