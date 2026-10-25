// Copyright (c) 2026 Kenneth Stott
// Canary: ad130446-9e80-46e8-8ac0-a63e8315bedf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: one profile run drawn as terrain — a ridge per column that can be orbited and zoomed,
// beside the selected column's flat histogram. The expand button opens the same view at 95%.

import { useMemo, useState } from "react";
import { ResetSettingsButton } from "../ResetSettingsButton";
import { useGraphSettings } from "../graphSettings";
import { useTranslation } from "react-i18next";
import { Button, Chip, Group, SegmentedControl, Select, Text } from "@mantine/core";
import type { ProfileRunResults } from "../../../../api/profiler";
import { ExpandButton, Expandable3D } from "../Expandable3D";
import {
  HEIGHT_MODES,
  TERRAIN_ORDERS,
  buildTerrain,
  orderColumns,
  type HeightMode,
  type TerrainColumn,
  type TerrainOrder,
} from "./terrainModel";
import type { TerrainHover, TerrainOptions } from "./terrainScene";
import { TerrainDetail } from "./TerrainDetail";
import type { CameraCommand } from "../Stage3D";
import { TerrainStage } from "./TerrainStage";
import "../profile3d.css";

const ORDERS: TerrainOrder[] = ["schema", "nulls", "skew", "distinct", "drift"];

interface SharedState {
  /** What the view's exports are named after. */
  exportName: string;
  columns: TerrainColumn[];
  order: string[];
  options: TerrainOptions;
  setOptions: (o: TerrainOptions) => void;
  orderBy: TerrainOrder;
  setOrderBy: (o: TerrainOrder) => void;
  rotate: boolean;
  setRotate: (on: boolean) => void;
  resetSettings: () => void;
  selected: string | null;
  setSelected: (c: string) => void;
  excluded: ReadonlySet<string>;
  /** ``order`` without the excluded columns: what the terrain draws, side by side. */
  drawn: string[];
  include: (names: string[], included: boolean) => void;
  hover: TerrainHover | null;
  setHover: (h: TerrainHover | null) => void;
}

// One drawn copy of the terrain with its own camera: the tab's, and the expanded modal's.
function TerrainView({
  s,
  expanded,
  onExpand,
}: {
  s: SharedState;
  expanded: boolean;
  onExpand?: () => void;
}) {
  const { t } = useTranslation();
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const move = (kind: CameraCommand["kind"], target: string | null) =>
    setCommand((c) => ({ kind, target, seq: c == null ? 1 : c.seq + 1 }));
  return (
    <div
      className={`p3d${expanded ? " is-expanded" : ""}`}
      data-testid={expanded ? "terrain-expanded" : "terrain"}
    >
      <Group className="p3d-toolbar" gap="xs" wrap="wrap">
        <SegmentedControl
          size="xs"
          value={s.options.mode}
          onChange={(v) => s.setOptions({ ...s.options, mode: v as HeightMode })}
          data={[
            { value: "peak", label: t("profileTerrain.heightPeak") },
            { value: "share", label: t("profileTerrain.heightShare") },
          ]}
          aria-label={t("profileTerrain.height")}
          data-testid="terrain-mode"
        />
        <Select
          size="xs"
          w={170}
          value={s.orderBy}
          onChange={(v) => v && s.setOrderBy(v as TerrainOrder)}
          data={ORDERS.map((o) => ({ value: o, label: t(`profileTerrain.order_${o}`) }))}
          allowDeselect={false}
          aria-label={t("profileTerrain.order")}
          data-testid="terrain-order"
        />
        <Chip
          size="xs"
          checked={s.options.log}
          onChange={(on) => s.setOptions({ ...s.options, log: on })}
          data-testid="terrain-log"
        >
          {t("profileTerrain.log")}
        </Chip>
        <Chip
          size="xs"
          checked={s.options.quantiles}
          onChange={(on) => s.setOptions({ ...s.options, quantiles: on })}
          data-testid="terrain-quantiles"
        >
          {t("profileTerrain.quantiles")}
        </Chip>
        <Chip size="xs" checked={s.rotate} onChange={s.setRotate} data-testid="terrain-rotate">
          {t("profile3d.rotate")}
        </Chip>
        <Button
          size="xs"
          variant="default"
          onClick={() => move("reset", null)}
          data-testid="terrain-reset"
        >
          {t("profile3d.resetView")}
        </Button>
        <ResetSettingsButton onReset={s.resetSettings} testId="terrain" />
        {onExpand && <ExpandButton onClick={onExpand} testId="terrain-expand" />}
      </Group>
      <div className="p3d-body">
        <TerrainStage
          columns={s.columns}
          order={s.drawn}
          options={s.options}
          rotate={s.rotate}
          selected={s.selected}
          hover={s.hover}
          command={command}
          onHover={s.setHover}
          onSelect={s.setSelected}
          onUserMove={() => s.setRotate(false)}
          exportName={s.exportName}
        />
        <TerrainDetail
          columns={s.columns}
          order={s.order}
          selected={s.selected}
          hover={s.hover}
          onSelect={s.setSelected}
          onHover={s.setHover}
          onFocus={(c) => {
            s.setRotate(false);
            move("focus", c);
          }}
          excluded={s.excluded}
          onInclude={s.include}
        />
      </div>
    </div>
  );
}

export function ProfileTerrain({
  results,
  tableId,
  tableName,
}: {
  results: ProfileRunResults;
  tableId: number;
  tableName: string;
}) {
  const { t } = useTranslation();
  const columns = useMemo(() => buildTerrain(results), [results]);
  // Remembered for this table's terrain (graphSettings.ts): the height mode, log height, quantiles,
  // the column order, rotation and the columns left out. A left-out column the profile no longer
  // has is dropped.
  const [saved, save, resetSettings] = useGraphSettings(
    { kind: "profile-terrain", subject: tableId },
    {
      mode: "peak",
      log: false as boolean,
      quantiles: true as boolean,
      orderBy: "schema",
      rotate: true as boolean,
      excluded: [] as string[],
    },
  );
  const options: TerrainOptions = useMemo(
    () => ({
      mode: (HEIGHT_MODES.includes(saved.mode as HeightMode) ? saved.mode : "peak") as HeightMode,
      log: saved.log as boolean,
      quantiles: saved.quantiles as boolean,
    }),
    [saved.mode, saved.log, saved.quantiles],
  );
  const setOptions = (o: TerrainOptions) => {
    if (o.mode !== options.mode) save("mode", o.mode);
    if (o.log !== options.log) save("log", o.log);
    if (o.quantiles !== options.quantiles) save("quantiles", o.quantiles);
  };
  const orderBy = (
    TERRAIN_ORDERS.includes(saved.orderBy as TerrainOrder) ? saved.orderBy : "schema"
  ) as TerrainOrder;
  const setOrderBy = (o: TerrainOrder) => save("orderBy", o);
  const rotate = saved.rotate as boolean;
  const setRotate = (on: boolean) => save("rotate", on);
  const [picked, setSelected] = useState<string | null>(null);
  const [hover, setHover] = useState<TerrainHover | null>(null);
  const savedExcluded = saved.excluded as string[];
  const excluded: ReadonlySet<string> = useMemo(
    () => new Set(savedExcluded.filter((n) => columns.some((c) => c.name === n))),
    [savedExcluded, columns],
  );
  const include = (names: string[], included: boolean) => {
    const next = new Set(excluded);
    for (const n of names) {
      if (included) next.delete(n);
      else next.add(n);
    }
    save("excluded", [...next]);
  };
  const order = useMemo(() => orderColumns(columns, orderBy), [columns, orderBy]);
  const drawn = useMemo(() => order.filter((n) => !excluded.has(n)), [order, excluded]);
  // A pick from an earlier run that this run did not profile falls back to the first column.
  const selected = columns.some((c) => c.name === picked) ? picked : (columns[0]?.name ?? null);

  if (columns.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="terrain-empty">
        {t("profileTerrain.noColumns")}
      </Text>
    );
  }
  const s: SharedState = {
    exportName: `${tableName}-terrain`,
    columns,
    order,
    options,
    setOptions,
    orderBy,
    setOrderBy,
    rotate,
    setRotate,
    selected,
    setSelected,
    excluded,
    drawn,
    include,
    hover,
    setHover,
    resetSettings,
  };
  return (
    <Expandable3D
      title={t("profileTerrain.expandTitle", { table: tableName })}
      testId="terrain"
      render={(expanded, onExpand) => <TerrainView s={s} expanded={expanded} onExpand={onExpand} />}
    />
  );
}
