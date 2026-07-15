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
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  NumberInput,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, Pause, Play, Maximize2, X } from "lucide-react";
import {
  updateSettings,
  reloadQueryEngineCatalog,
  restartQueryEngine,
  recomputeSchemaClusters,
} from "../../api/admin";
import type { PlatformSettings } from "../../api/admin";

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

  const needle = filter.toLowerCase();
  const visible = needle
    ? traces.filter(
        (t) =>
          t.name.toLowerCase().includes(needle) ||
          t.status.toLowerCase().includes(needle) ||
          t.trace_id.toLowerCase().includes(needle),
      )
    : traces;

  return (
    <div className="trace-feed-list" ref={listRef}>
      {visible.length === 0 ? (
        <Text className="trace-empty" size="sm" c="dimmed">
          {traces.length === 0 ? t("observabilityTab.noSpansYet") : t("observabilityTab.noSpansMatch")}
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
      <TextInput
        className="trace-filter"
        placeholder={t("observabilityTab.filterPlaceholder")}
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        onClick={(e) => e.stopPropagation()}
        aria-label={t("observabilityTab.filterPlaceholder")}
        size="xs"
      />
      <Group flex={1} justify="flex-end" align="center" gap="0.4rem">
        <ActionIcon
          className="trace-pause-btn"
          style={{ marginLeft: "0.5rem" }}
          onClick={() => setPaused((p) => !p)}
          aria-label={paused ? t("observabilityTab.resume") : t("observabilityTab.pause")}
          title={paused ? t("observabilityTab.resume") : t("observabilityTab.pause")}
          variant="subtle"
        >
          {paused ? <Play size={14} /> : <Pause size={14} />}
        </ActionIcon>
        <ActionIcon
          className="trace-expand-btn"
          onClick={(e) => { e.stopPropagation(); setExpanded((v) => !v); }}
          aria-label={expanded ? t("observabilityTab.collapse") : t("observabilityTab.expand")}
          title={expanded ? t("observabilityTab.collapse") : t("observabilityTab.expand")}
          variant="subtle"
        >
          {expanded ? <X size={14} /> : <Maximize2 size={14} />}
        </ActionIcon>
      </Group>
    </>
  );

  return (
    <>
      <div className="trace-feed">
        <div className="trace-feed-header">
          {headerContent}
        </div>
        <TraceList traces={traces} filter={filter} selected={selected} setSelected={setSelected} listRef={listRef} />
      </div>
      <Modal
        opened={expanded}
        onClose={() => setExpanded(false)}
        size="90vw"
        title={t("observabilityTab.traces")}
        transitionProps={{ duration: 0 }}
      >
        <div style={{ height: "80vh", overflow: "auto" }}>
          <TraceList traces={traces} filter={filter} selected={selected} setSelected={setSelected} />
        </div>
      </Modal>
    </>
  );
}

export function ObservabilityTab({ settings, setSettings }: ObsTabProps) {
  const { t } = useTranslation();
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  const update = (key: keyof typeof settings.otel, value: unknown) =>
    setSettings({ ...settings, otel: { ...settings.otel, [key]: value } });

  const save = async () => {
    setSaving(true);
    setMsg("");
    try {
      const result = await updateSettings({ otel: settings.otel });
      setMsg(`Saved: ${result.updated.join(", ")}`);
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : t("observabilityTab.saveFailed"));
    } finally {
      setSaving(false);
    }
  };

  const active = Boolean(settings.otel.endpoint);

  return (
    <div className="observability-layout">
      <div className="settings-section">
        <Title order={4}>{t("observabilityTab.otelTitle")}</Title>
        <Text c="dimmed" size="sm" mb="md">
          {t("observabilityTab.statusLabel")}{" "}
          <Text component="strong" c={active ? "var(--success)" : "dimmed"} span>
            {active
              ? t("observabilityTab.statusExporting", { endpoint: settings.otel.endpoint })
              : t("observabilityTab.statusActiveNoCollector")}
          </Text>
        </Text>
        <Stack gap="md">
          <TextInput
            label={t("observabilityTab.endpointLabel")}
            value={settings.otel.endpoint}
            onChange={(e) => update("endpoint", e.target.value)}
            placeholder="http://otel-collector:4317"
            description={t("observabilityTab.endpointHelp")}
          />
          <TextInput
            label={t("observabilityTab.serviceNameLabel")}
            value={settings.otel.service_name}
            onChange={(e) => update("service_name", e.target.value)}
            placeholder="provisa"
            description={t("observabilityTab.serviceNameHelp")}
          />
          <NumberInput
            label={t("observabilityTab.sampleRateLabel")}
            min={0}
            max={1}
            step={0.01}
            value={settings.otel.sample_rate}
            onChange={(v) => update("sample_rate", typeof v === "number" ? v : 0)}
            description={t("observabilityTab.sampleRateHelp")}
          />
          <TextInput
            label={t("observabilityTab.logLevelLabel")}
            value={settings.otel.log_level}
            onChange={(e) => update("log_level", e.target.value)}
            placeholder="WARNING"
            description={t("observabilityTab.logLevelHelp")}
          />
        </Stack>

        <Title order={4} mt="lg">{t("observabilityTab.pipelineTitle")}</Title>
        <Text c="dimmed" size="sm" mb="md">
          {t("observabilityTab.pipelineIntro")}
        </Text>
        <Stack gap="md">
          <TextInput
            label={t("observabilityTab.s3EndpointLabel")}
            value={settings.otel.s3_endpoint}
            onChange={(e) => update("s3_endpoint", e.target.value)}
            placeholder="http://minio:9000"
            description={t("observabilityTab.s3EndpointHelp")}
          />
          <TextInput
            label={t("observabilityTab.compactCronLabel")}
            value={settings.otel.compact_cron}
            onChange={(e) => update("compact_cron", e.target.value)}
            placeholder="* * * * *"
            description={t("observabilityTab.compactCronHelp")}
          />
          <NumberInput
            label={t("observabilityTab.compactBatchSizeLabel")}
            min={1}
            value={settings.otel.compact_batch_size}
            onChange={(v) => update("compact_batch_size", typeof v === "number" ? v : 0)}
            description={t("observabilityTab.compactBatchSizeHelp")}
          />
          <NumberInput
            label={t("observabilityTab.compactFileChunkLabel")}
            min={1}
            value={settings.otel.compact_file_chunk}
            onChange={(v) => update("compact_file_chunk", typeof v === "number" ? v : 0)}
            description={t("observabilityTab.compactFileChunkHelp")}
          />
          <NumberInput
            label={t("observabilityTab.opsSnapshotRetentionLabel")}
            min={0}
            value={settings.otel.ops_snapshot_retention_hours ?? ""}
            onChange={(v) =>
              update("ops_snapshot_retention_hours", v === "" ? null : Number(v))
            }
            description={t("observabilityTab.opsSnapshotRetentionHelp")}
          />
          <NumberInput
            label={t("observabilityTab.spanExportDelayLabel")}
            min={0}
            value={settings.otel.span_export_delay_millis}
            onChange={(v) => update("span_export_delay_millis", typeof v === "number" ? v : 0)}
            description={t("observabilityTab.spanExportDelayHelp")}
          />
          <NumberInput
            label={t("observabilityTab.otlp2parquetMaxAgeLabel")}
            min={0}
            value={settings.otel.otlp2parquet_max_age_secs}
            onChange={(v) => update("otlp2parquet_max_age_secs", typeof v === "number" ? v : 0)}
            description={t("observabilityTab.otlp2parquetMaxAgeHelp")}
          />
          <NumberInput
            label={t("observabilityTab.collectorBatchTimeoutLabel")}
            min={0}
            value={settings.otel.collector_batch_timeout_ms}
            onChange={(v) => update("collector_batch_timeout_ms", typeof v === "number" ? v : 0)}
            description={t("observabilityTab.collectorBatchTimeoutHelp")}
          />
        </Stack>
        <Group mt="md" gap="0.75rem" align="center">
          <ActionIcon
            className="btn-primary"
            onClick={save}
            disabled={saving}
            aria-label={t("observabilityTab.saveButton")}
            title={t("observabilityTab.saveButton")}
          >
            {saving ? <Loader size={14} /> : <Check size={14} />}
          </ActionIcon>
          {msg && <Text className="upload-msg" size="sm">{msg}</Text>}
        </Group>
        <Text mt="lg" size="xs" c="dimmed">
          {t("observabilityTab.restartNote")}
        </Text>

        <Title order={4} mt="lg">{t("observabilityTab.supportTitle")}</Title>
        <Text c="dimmed" size="sm" mb="md">
          {t("observabilityTab.supportIntro")}
        </Text>
        <Stack gap="md">
          <TextInput
            label={t("observabilityTab.supportEndpointLabel")}
            value={settings.otel.support_endpoint}
            onChange={(e) => update("support_endpoint", e.target.value)}
            placeholder="https://otel.provisa.io:4318"
            description={t("observabilityTab.supportEndpointHelp")}
          />
          <Checkbox
            checked={settings.otel.support_redact_sql_literals}
            onChange={(e) => update("support_redact_sql_literals", e.target.checked)}
            label={t("observabilityTab.redactSqlLabel")}
          />
          <TextInput
            label={t("observabilityTab.redactAttrsLabel")}
            value={(settings.otel.support_redact_attributes ?? []).join(", ")}
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
            description={t("observabilityTab.redactAttrsHelp")}
          />
        </Stack>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
        <TraceFeed />
        <QueryEngineActions />
      </div>
    </div>
  );
}

function QueryEngineActions() {
  const { t } = useTranslation();
  const [reloadStatus, setReloadStatus] = useState<string>("");
  const [restartStatus, setRestartStatus] = useState<string>("");
  const [clusterStatus, setClusterStatus] = useState<string>("");
  const [reloading, setReloading] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [reclustering, setReclustering] = useState(false);

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

  const handleRecluster = async () => {
    setReclustering(true);
    setClusterStatus("");
    try {
      const result = await recomputeSchemaClusters();
      setClusterStatus(t("observabilityTab.clustered", { count: result.tables_clustered }));
    } catch (e: unknown) {
      setClusterStatus(e instanceof Error ? e.message : t("observabilityTab.failed"));
    } finally {
      setReclustering(false);
    }
  };

  return (
    <div className="settings-section">
      <Title order={4}>{t("observabilityTab.queryEngineTitle")}</Title>
      <Group gap="0.75rem" align="center" wrap="wrap" mt="sm">
        <Button className="btn-primary" onClick={handleReload} disabled={reloading}>
          {reloading ? t("observabilityTab.reloading") : t("observabilityTab.reloadCatalog")}
        </Button>
        <Button color="yellow" className="btn-warning" onClick={handleRestart} disabled={restarting}>
          {restarting ? t("observabilityTab.restarting") : t("observabilityTab.restartEngine")}
        </Button>
        <Button variant="default" className="btn-secondary" onClick={handleRecluster} disabled={reclustering}>
          {reclustering ? t("observabilityTab.clustering") : t("observabilityTab.recomputeClusters")}
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
      {clusterStatus && (
        <Text className="upload-msg" size="sm" mt="0.5rem">
          {clusterStatus}
        </Text>
      )}
    </div>
  );
}
