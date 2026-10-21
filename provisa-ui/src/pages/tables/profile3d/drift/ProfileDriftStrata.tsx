// Copyright (c) 2026 Kenneth Stott
// Canary: cfc2365c-51ea-4f1c-bd9c-c7946e096edc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: drift strata — one column's distribution in the selected run and the runs before it,
// stacked oldest to newest, with the median traced through them and the runs that drifted marked.

import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Chip, Group, Loader, Select, Text } from "@mantine/core";
import { fetchProfileRun, type ProfileRun, type ProfileRunResults } from "../../../../api/profiler";
import { ExpandButton, Expandable3D } from "../Expandable3D";
import { Stage3D, type CameraCommand } from "../Stage3D";
import { useLazyScene } from "../useLazyScene";
import { pctText } from "../terrain/terrainModel";
import { buildStrata, strataColumns, type StrataModel, type StrataRunInput } from "./driftModel";
import type { DriftScene, StrataHover } from "./driftScene";
import "../profile3d.css";

/** The runs stacked: the selected one and up to this many before it, all told. */
export const STRATA_RUNS = 8;

interface Shared {
  columns: string[];
  column: string;
  setColumn: (c: string) => void;
  model: StrataModel;
  selectedRun: number;
  rotate: boolean;
  setRotate: (on: boolean) => void;
  hover: StrataHover | null;
  setHover: (h: StrataHover | null) => void;
}

const runTimeLabel = (iso: string) =>
  new Date(iso).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });

function DriftStage({ s, command }: { s: Shared; command: CameraCommand | null }) {
  const { t } = useTranslation();
  const [tip, setTip] = useState<{ h: StrataHover; x: number; y: number } | null>(null);
  const latest = useRef(s);
  useEffect(() => {
    latest.current = s;
  });
  const { wrapRef, canvasRef, labelsRef, scene, error } = useLazyScene<DriftScene>(
    (canvas, labels, theme) =>
      import("./driftScene").then(({ DriftScene }) => {
        const sc = new DriftScene(
          canvas,
          labels,
          {
            onHover: (h, x, y) => {
              setTip(h ? { h, x, y } : null);
              latest.current.setHover(h);
            },
            onSelect: () => undefined,
            onUserMove: () => latest.current.setRotate(false),
          },
          theme,
          (i) => runTimeLabel(latest.current.model.runs[i].runTime),
          {
            low: t("profileTerrain.axisLow"),
            high: t("profileTerrain.axisHigh"),
            nulls: t("profileTerrain.axisNulls"),
          },
        );
        sc.setData(latest.current.model, true);
        return sc;
      }),
  );
  const drawn = useRef<StrataModel | null>(null);
  useEffect(() => {
    if (!scene) return;
    if (drawn.current === null) drawn.current = latest.current.model;
    else if (drawn.current !== s.model) {
      drawn.current = s.model;
      scene.setData(s.model, false);
    }
  }, [scene, s.model]);
  useEffect(() => scene?.setSelected(s.selectedRun), [scene, s.selectedRun]);
  useEffect(() => scene?.setHover(s.hover), [scene, s.hover]);
  useEffect(() => scene?.setRotate(s.rotate), [scene, s.rotate]);
  useEffect(() => {
    if (scene && command != null) scene.resetView();
  }, [scene, command]);

  const run = tip ? s.model.runs[tip.h.run] : undefined;
  let body = null;
  if (tip && run) {
    const share =
      tip.h.bin === "null" ? run.nullShare : tip.h.bin === "run" ? null : run.shares[tip.h.bin];
    body = (
      <>
        <b>{runTimeLabel(run.runTime)}</b>
        {tip.h.bin !== "run" && (
          <span>
            {tip.h.bin === "null" ? t("profileTerrain.tipNull") : s.model.labels[tip.h.bin]}
          </span>
        )}
        {share != null && (
          <span className="p3d-num">
            {t(tip.h.bin === "null" ? "profileStrata.tipNullShare" : "profileStrata.tipShare", {
              pct: pctText(share),
            })}
          </span>
        )}
        {run.driftReason != null && (
          <span className="p3d-warn">
            {t("profileStrata.drifted", { reason: run.driftReason })}
          </span>
        )}
      </>
    );
  }

  return (
    <Stage3D
      wrapRef={wrapRef}
      canvasRef={canvasRef}
      labelsRef={labelsRef}
      ready={scene != null}
      error={error}
      canvasLabel={t("profileStrata.canvasLabel")}
      tip={tip && body ? { x: tip.x, y: tip.y, body } : null}
      testId="strata-stage"
      legend={
        <>
          <span>
            <i className="p3d-ramp" />
            {t("profileStrata.legendShare")}
          </span>
          {s.model.kind === "sketch" && (
            <span>
              <i className="p3d-sw p3d-sw-mid" />
              {t("profileStrata.legendMedian")}
            </span>
          )}
          <span>
            <i className="p3d-sw p3d-sw-warm" />
            {t("profileStrata.legendDrift")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-null" />
            {t("profileTerrain.legendNulls")}
          </span>
        </>
      }
    />
  );
}

function DriftDetail({ s }: { s: Shared }) {
  const { t } = useTranslation();
  const runs = [...s.model.runs.map((r, i) => ({ r, i }))].reverse();
  return (
    <div className="p3d-detail">
      <section data-testid="strata-detail" aria-live="polite">
        <div className="p3d-col-head">
          <h4>{s.model.column}</h4>
          <span className="p3d-kind">{t(`profileTerrain.kind_${s.model.family}`)}</span>
        </div>
        <p className="p3d-note">{t(`profileStrata.binning_${s.model.kind}`)}</p>
      </section>
      <section>
        <h5 className="p3d-section-title">{t("profileStrata.runs")}</h5>
        <div className="p3d-list">
          {runs.map(({ r, i }) => (
            <div
              key={r.runId}
              className={`p3d-row p3d-row-static${i === s.selectedRun ? " is-current" : ""}${s.hover?.run === i ? " is-on" : ""}`}
              onMouseEnter={() => s.setHover({ run: i, bin: "run" })}
              onMouseLeave={() => s.setHover(null)}
              data-testid={`strata-run-${r.runId}`}
            >
              <span className="p3d-row-name">
                {runTimeLabel(r.runTime)}
                <small className={r.driftReason != null ? "is-warn" : undefined}>
                  {r.driftReason != null
                    ? t("profileStrata.drifted", { reason: r.driftReason })
                    : r.shares.length === 0
                      ? t("profileStrata.noValues")
                      : t("profileStrata.steady")}
                </small>
              </span>
              <span className="p3d-num">{r.median ?? ""}</span>
              <span className="p3d-num">{r.nullShare != null ? pctText(r.nullShare) : "—"}</span>
            </div>
          ))}
        </div>
        <p className="p3d-note">{t("profileStrata.columnsNote")}</p>
      </section>
    </div>
  );
}

function DriftView({
  s,
  expanded,
  onExpand,
}: {
  s: Shared;
  expanded: boolean;
  onExpand?: () => void;
}) {
  const { t } = useTranslation();
  const [command, setCommand] = useState<CameraCommand | null>(null);
  return (
    <div
      className={`p3d${expanded ? " is-expanded" : ""}`}
      data-testid={expanded ? "strata-expanded" : "strata"}
    >
      <Group className="p3d-toolbar" gap="xs" wrap="wrap">
        <Select
          size="xs"
          w={220}
          value={s.column}
          onChange={(v) => v && s.setColumn(v)}
          data={s.columns}
          allowDeselect={false}
          searchable
          aria-label={t("profileStrata.column")}
          data-testid="strata-column"
        />
        <Chip size="xs" checked={s.rotate} onChange={s.setRotate} data-testid="strata-rotate">
          {t("profile3d.rotate")}
        </Chip>
        <Button
          size="xs"
          variant="default"
          onClick={() =>
            setCommand((c) => ({ kind: "reset", target: null, seq: c == null ? 1 : c.seq + 1 }))
          }
          data-testid="strata-reset"
        >
          {t("profile3d.resetView")}
        </Button>
        {onExpand && <ExpandButton onClick={onExpand} testId="strata-expand" />}
      </Group>
      <div className="p3d-body">
        <DriftStage s={s} command={command} />
        <DriftDetail s={s} />
      </div>
    </div>
  );
}

function DriftLoaded({ inputs, tableName }: { inputs: StrataRunInput[]; tableName: string }) {
  const { t } = useTranslation();
  const columns = useMemo(() => strataColumns(inputs), [inputs]);
  const preferred = useMemo(() => {
    // Open on a column the selected run found drifting, when there is one.
    const newest = inputs[inputs.length - 1].results;
    const d = (newest.drift ?? []).find(
      (r) =>
        r.drifting === true && r.column_name != null && columns.includes(String(r.column_name)),
    );
    return d ? String(d.column_name) : columns[0];
  }, [inputs, columns]);
  const [picked, setColumn] = useState<string | null>(null);
  const column = picked != null && columns.includes(picked) ? picked : preferred;
  const model = useMemo(() => buildStrata(inputs, column), [inputs, column]);
  const [rotate, setRotate] = useState(false);
  const [hover, setHover] = useState<StrataHover | null>(null);
  const s: Shared = {
    columns,
    column,
    setColumn: (c) => {
      setHover(null);
      setColumn(c);
    },
    model,
    selectedRun: inputs.length - 1,
    rotate,
    setRotate,
    hover,
    setHover,
  };
  return (
    <Expandable3D
      title={t("profileStrata.expandTitle", { table: tableName })}
      testId="strata"
      render={(expanded, onExpand) => <DriftView s={s} expanded={expanded} onExpand={onExpand} />}
    />
  );
}

export function ProfileDriftStrata({
  tableId,
  role,
  runs,
  selectedRunId,
  results,
  tableName,
}: {
  tableId: number;
  role: string;
  runs: ProfileRun[];
  selectedRunId: string;
  results: ProfileRunResults;
  tableName: string;
}) {
  const { t } = useTranslation();
  const stack = useMemo(() => {
    const selected = runs.find((r) => r.run_id === selectedRunId) as ProfileRun;
    return runs
      .filter((r) => r.status === "succeeded" && r.run_time <= selected.run_time)
      .sort((a, b) => b.run_time.localeCompare(a.run_time))
      .slice(0, STRATA_RUNS)
      .reverse();
  }, [runs, selectedRunId]);
  const key = stack.map((r) => r.run_id).join(",");
  const [loaded, setLoaded] = useState<{ key: string; inputs: StrataRunInput[] } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    Promise.all(
      stack.map((r) =>
        (r.run_id === selectedRunId
          ? Promise.resolve(results)
          : fetchProfileRun(tableId, r.run_id, role)
        ).then((res): StrataRunInput => ({ runId: r.run_id, runTime: r.run_time, results: res })),
      ),
    )
      .then((inputs) => {
        if (!cancelled) setLoaded({ key, inputs });
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [stack, key, selectedRunId, results, tableId, role]);

  if (error) {
    return (
      <Alert color="red" data-testid="strata-error">
        {error}
      </Alert>
    );
  }
  if (loaded == null || loaded.key !== key)
    return <Loader size="sm" data-testid="strata-loading" />;
  if (strataColumns(loaded.inputs).length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="strata-empty">
        {t("profileStrata.none")}
      </Text>
    );
  }
  return <DriftLoaded inputs={loaded.inputs} tableName={tableName} />;
}
