// Copyright (c) 2026 Kenneth Stott
// Canary: 3c062a6e-e74c-4c85-b670-5eb9cce6c868
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the joint cloud — a column and its parents in the dependency network as a cube of row
// counts, coloured by rows or by lift, beside the combinations that most beat independence.

import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Chip, Group, SegmentedControl, Select, Text } from "@mantine/core";
import type { ProfileRunResults } from "../../../../api/profiler";
import { ExpandButton, Expandable3D } from "../Expandable3D";
import { Stage3D, type CameraCommand } from "../Stage3D";
import { useLazyScene } from "../useLazyScene";
import { pctText } from "../terrain/terrainModel";
import { buildJoint, jointTargets, type JointModel } from "./jointModel";
import type { JointColor, JointHover, JointScene } from "./jointScene";
import "../profile3d.css";

const LISTED = 8;

interface Shared {
  targets: string[];
  target: string;
  setTarget: (c: string) => void;
  model: JointModel;
  color: JointColor;
  setColor: (c: JointColor) => void;
  rotate: boolean;
  setRotate: (on: boolean) => void;
  hover: JointHover | null;
  setHover: (h: JointHover | null) => void;
  slotName: (axis: number, slot: number) => string;
}

function useSlotName(model: JointModel) {
  const { t } = useTranslation();
  return (axis: number, slot: number): string => {
    const a = model.axes[axis];
    const s = a.slots[slot];
    if (a.kind === "single") return "";
    if (s.other) return t("profileJoint.otherSlot", { count: Number(s.value) });
    if (s.value == null) return t("profileJoint.nullSlot");
    return a.kind === "decile" ? t("profileJoint.decile", { n: Number(s.value) + 1 }) : s.value;
  };
}

function JointStage({ s, command }: { s: Shared; command: CameraCommand | null }) {
  const { t } = useTranslation();
  const [tip, setTip] = useState<{ h: JointHover; x: number; y: number } | null>(null);
  const latest = useRef(s);
  useEffect(() => {
    latest.current = s;
  });
  const { wrapRef, canvasRef, labelsRef, scene, error } = useLazyScene<JointScene>(
    (canvas, labels, theme) =>
      import("./jointScene").then(({ JointScene }) => {
        const l = latest.current;
        const sc = new JointScene(
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
          l.color,
          l.slotName,
        );
        sc.setData(l.model, l.slotName, true);
        return sc;
      }),
  );
  const drawn = useRef<JointModel | null>(null);
  useEffect(() => {
    if (!scene) return;
    if (drawn.current === null) drawn.current = latest.current.model;
    else if (drawn.current !== s.model) {
      drawn.current = s.model;
      scene.setData(s.model, latest.current.slotName, true);
    }
  }, [scene, s.model]);
  useEffect(() => scene?.setColor(s.color), [scene, s.color]);
  useEffect(() => scene?.setHover(s.hover), [scene, s.hover]);
  useEffect(() => scene?.setRotate(s.rotate), [scene, s.rotate]);
  useEffect(() => {
    if (scene && command != null) scene.resetView();
  }, [scene, command]);

  const cell = tip ? s.model.cells[tip.h.cell] : undefined;
  const [ax, ay, az] = s.model.axes;
  const body = cell ? (
    <>
      <b>
        {ay.column} = {s.slotName(1, cell.y)}
      </b>
      <span>
        {ax.column} = {s.slotName(0, cell.x)}
      </span>
      {az.kind !== "single" && (
        <span>
          {az.column} = {s.slotName(2, cell.z)}
        </span>
      )}
      <span className="p3d-num">
        {t("profileJoint.tipRows", {
          rows: cell.count.toLocaleString(),
          pct: pctText(cell.count / s.model.total),
          lift: cell.lift.toFixed(2),
        })}
      </span>
    </>
  ) : null;

  return (
    <Stage3D
      wrapRef={wrapRef}
      canvasRef={canvasRef}
      labelsRef={labelsRef}
      ready={scene != null}
      error={error}
      canvasLabel={t("profileJoint.canvasLabel")}
      tip={tip && body ? { x: tip.x, y: tip.y, body } : null}
      testId="joint-stage"
      legend={
        s.color === "rows" ? (
          <span>
            <i className="p3d-ramp" />
            {t("profileJoint.legendRows")}
          </span>
        ) : (
          <span>
            <i className="p3d-diverging" />
            {t("profileJoint.legendLift")}
          </span>
        )
      }
    />
  );
}

function JointDetail({ s }: { s: Shared }) {
  const { t } = useTranslation();
  const m = s.model;
  const [ax, ay, az] = m.axes;
  const indexed = m.cells.map((c, i) => ({ c, i }));
  // Combinations worth reading: at least 0.5% of rows, so a lift is not one row's noise.
  const floor = m.total * 0.005;
  const over = indexed
    .filter((x) => x.c.count >= floor && x.c.lift > 1)
    .sort((a, b) => b.c.lift - a.c.lift)
    .slice(0, LISTED);
  const under = indexed
    .filter((x) => x.c.count >= floor && x.c.lift < 1)
    .sort((a, b) => a.c.lift - b.c.lift)
    .slice(0, LISTED);
  const combo = (i: number) => {
    const c = m.cells[i];
    const parts = [`${ay.column}=${s.slotName(1, c.y)}`, `${ax.column}=${s.slotName(0, c.x)}`];
    if (az.kind !== "single") parts.push(`${az.column}=${s.slotName(2, c.z)}`);
    return parts.join(" · ");
  };
  const list = (rows: { c: (typeof m.cells)[number]; i: number }[], testId: string) => (
    <div className="p3d-list" data-testid={testId}>
      {rows.map(({ c, i }) => (
        <div
          key={i}
          className={`p3d-row p3d-row-static${s.hover?.cell === i ? " is-on" : ""}`}
          onMouseEnter={() => s.setHover({ cell: i })}
          onMouseLeave={() => s.setHover(null)}
          data-testid={`joint-cell-${i}`}
        >
          <span className="p3d-row-name p3d-wrap">{combo(i)}</span>
          <span className="p3d-num">×{c.lift.toFixed(2)}</span>
          <span className="p3d-num">{pctText(c.count / m.total)}</span>
        </div>
      ))}
      {rows.length === 0 && <p className="p3d-note">{t("profileJoint.noneListed")}</p>}
    </div>
  );
  const stats: [string, string][] = [
    [t("profileJoint.stat_rows"), m.total.toLocaleString()],
    [t("profileJoint.stat_cells"), m.cells.length.toLocaleString()],
  ];
  if (m.mutualInformation != null)
    stats.push([t("profileJoint.stat_mi"), m.mutualInformation.toFixed(3)]);
  if (m.uncertainty != null)
    stats.push([t("profileJoint.stat_uncertainty"), pctText(m.uncertainty)]);
  return (
    <div className="p3d-detail">
      <section data-testid="joint-detail" aria-live="polite">
        <div className="p3d-col-head">
          <h4>{m.target}</h4>
        </div>
        <p className="p3d-note">
          {az.kind === "single"
            ? t("profileJoint.explainOne", { column: m.target, parent: ax.column })
            : t("profileJoint.explainTwo", {
                column: m.target,
                parent1: ax.column,
                parent2: az.column,
              })}
        </p>
        <dl className="p3d-stats">
          {stats.map(([k, v]) => (
            <div key={k}>
              <dt>{k}</dt>
              <dd>{v}</dd>
            </div>
          ))}
        </dl>
      </section>
      <section>
        <h5 className="p3d-section-title">{t("profileJoint.over")}</h5>
        {list(over, "joint-over")}
      </section>
      <section>
        <h5 className="p3d-section-title">{t("profileJoint.under")}</h5>
        {list(under, "joint-under")}
      </section>
      <p className="p3d-note">{t("profileJoint.note")}</p>
    </div>
  );
}

function JointView({
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
      data-testid={expanded ? "joint-expanded" : "joint"}
    >
      <Group className="p3d-toolbar" gap="xs" wrap="wrap">
        <Select
          size="xs"
          w={220}
          value={s.target}
          onChange={(v) => v && s.setTarget(v)}
          data={s.targets}
          allowDeselect={false}
          searchable
          aria-label={t("profileJoint.column")}
          data-testid="joint-target"
        />
        <SegmentedControl
          size="xs"
          value={s.color}
          onChange={(v) => s.setColor(v as JointColor)}
          data={[
            { value: "rows", label: t("profileJoint.colorRows") },
            { value: "lift", label: t("profileJoint.colorLift") },
          ]}
          aria-label={t("profileJoint.colorBy")}
          data-testid="joint-color"
        />
        <Chip size="xs" checked={s.rotate} onChange={s.setRotate} data-testid="joint-rotate">
          {t("profile3d.rotate")}
        </Chip>
        <Button
          size="xs"
          variant="default"
          onClick={() =>
            setCommand((c) => ({ kind: "reset", target: null, seq: c == null ? 1 : c.seq + 1 }))
          }
          data-testid="joint-reset"
        >
          {t("profile3d.resetView")}
        </Button>
        {onExpand && <ExpandButton onClick={onExpand} testId="joint-expand" />}
      </Group>
      <div className="p3d-body">
        <JointStage s={s} command={command} />
        <JointDetail s={s} />
      </div>
    </div>
  );
}

function JointLoaded({
  results,
  tableName,
  targets,
}: {
  results: ProfileRunResults;
  tableName: string;
  targets: string[];
}) {
  const { t } = useTranslation();
  const [picked, setTarget] = useState(targets[0]);
  const target = targets.includes(picked) ? picked : targets[0];
  // jointTargets lists only columns with joint counts, so the model exists.
  const model = useMemo(() => buildJoint(results, target) as JointModel, [results, target]);
  const [color, setColor] = useState<JointColor>("lift");
  const [rotate, setRotate] = useState(true);
  const [hover, setHover] = useState<JointHover | null>(null);
  const slotName = useSlotName(model);
  const s: Shared = {
    targets,
    target,
    setTarget: (c) => {
      setHover(null);
      setTarget(c);
    },
    model,
    color,
    setColor,
    rotate,
    setRotate,
    hover,
    setHover,
    slotName,
  };
  return (
    <Expandable3D
      title={t("profileJoint.expandTitle", { table: tableName })}
      testId="joint"
      render={(expanded, onExpand) => <JointView s={s} expanded={expanded} onExpand={onExpand} />}
    />
  );
}

export function ProfileJointCloud({
  results,
  tableName,
}: {
  results: ProfileRunResults;
  tableName: string;
}) {
  const { t } = useTranslation();
  const targets = useMemo(() => jointTargets(results), [results]);
  if (targets.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="joint-empty">
        {t("profileJoint.none")}
      </Text>
    );
  }
  return <JointLoaded results={results} tableName={tableName} targets={targets} />;
}
