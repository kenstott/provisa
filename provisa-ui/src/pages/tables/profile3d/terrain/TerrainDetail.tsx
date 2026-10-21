// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the flat side of the profile terrain — the selected column as a histogram (or a list of
// values) with its figures, and every column as a row. Hovering a bar lights the same slice in 3D.

import { useTranslation } from "react-i18next";
import { Button } from "@mantine/core";
import { formatValue, pctText, tOfValue, type TerrainColumn } from "./terrainModel";
import type { TerrainHover } from "./terrainScene";

const SHOWN_CATEGORIES = 12;

function Histogram({
  c,
  hover,
  onHover,
}: {
  c: TerrainColumn;
  hover: TerrainHover | null;
  onHover: (h: TerrainHover | null) => void;
}) {
  const B = c.bins.length;
  const VW = 320;
  const VH = 130;
  const pl = 4;
  const pt = 8;
  const pb = 22;
  const iw = VW - 2 * pl;
  const ih = VH - pt - pb;
  const bw = iw / B;
  const first = c.bins[0].label.split(" – ")[0];
  const last = c.bins[B - 1].label.split(" – ").slice(-1)[0];
  return (
    <svg
      className="p3d-hist"
      viewBox={`0 0 ${VW} ${VH}`}
      role="img"
      aria-label={c.name}
      data-testid="terrain-hist"
    >
      <line x1={pl} x2={VW - pl} y1={pt + ih} y2={pt + ih} stroke="var(--border)" />
      {c.bins.map((b, j) => {
        const bh = (b.count / c.maxCount) * ih;
        const on = hover?.column === c.name && hover.bin === j;
        return (
          <rect
            key={j}
            x={pl + j * bw + 0.4}
            y={pt + ih - bh}
            width={Math.max(0.6, bw - 0.8)}
            height={Math.max(0.5, bh)}
            fill={on ? "var(--p3d-hl)" : "var(--p3d-ramp-1)"}
            onMouseEnter={() => onHover({ column: c.name, bin: j })}
            onMouseLeave={() => onHover(null)}
            data-testid={`terrain-bar-${j}`}
          />
        );
      })}
      {c.quantiles &&
        (
          [
            [c.quantiles.p5, "var(--p3d-q-tail)"],
            [c.quantiles.p50, "var(--p3d-q-mid)"],
            [c.quantiles.p95, "var(--p3d-q-tail)"],
          ] as const
        ).map(([v, color], i) => {
          const x = pl + tOfValue(c, v) * iw;
          return (
            <line key={i} x1={x} x2={x} y1={pt} y2={pt + ih} stroke={color} strokeWidth={1.5} />
          );
        })}
      <text x={pl} y={VH - 6}>
        {first}
      </text>
      <text x={VW - pl} y={VH - 6} textAnchor="end">
        {last}
      </text>
    </svg>
  );
}

function Spark({ c }: { c: TerrainColumn }) {
  if (c.bins.length === 0)
    return <svg className="p3d-spark" viewBox="0 0 72 22" aria-hidden="true" />;
  const B = c.bins.length;
  const pts = c.bins.map(
    (b, j) =>
      `${(((j + 0.5) / B) * 72).toFixed(1)} ${(21 - (b.count / c.maxCount) * 19).toFixed(1)}`,
  );
  return (
    <svg className="p3d-spark" viewBox="0 0 72 22" aria-hidden="true">
      <path
        d={`M0 21 L${pts.join(" L")} L72 21 Z`}
        fill="var(--p3d-ramp-2)"
        stroke="var(--p3d-ramp-0)"
        strokeWidth={0.8}
      />
    </svg>
  );
}

export function TerrainDetail({
  columns,
  order,
  selected,
  hover,
  onSelect,
  onHover,
  onFocus,
}: {
  columns: TerrainColumn[];
  order: string[];
  selected: string | null;
  hover: TerrainHover | null;
  onSelect: (column: string) => void;
  onHover: (h: TerrainHover | null) => void;
  onFocus: (column: string) => void;
}) {
  const { t } = useTranslation();
  const c = columns.find((x) => x.name === selected);
  const flagText = (col: TerrainColumn) =>
    col.flags.map((f) => t(`profileTerrain.${f.key}`, f.params));

  const stats: [string, string][] = [];
  if (c) {
    if (c.rows != null) stats.push([t("profileTerrain.stat_rows"), c.rows.toLocaleString()]);
    if (c.nullCount != null && c.nullShare != null)
      stats.push([
        t("profileTerrain.stat_nulls"),
        `${c.nullCount.toLocaleString()} · ${pctText(c.nullShare)}`,
      ]);
    else if (c.nullShare != null)
      stats.push([t("profileTerrain.stat_nulls"), pctText(c.nullShare)]);
    if (c.distinct != null)
      stats.push([t("profileTerrain.stat_distinct"), c.distinct.toLocaleString()]);
    if (c.min != null) stats.push([t("profileTerrain.stat_min"), c.min]);
    if (c.quantiles) {
      const span = c.hi != null && c.lo != null ? c.hi - c.lo : undefined;
      stats.push([t("profileTerrain.stat_p5"), formatValue(c.family, c.quantiles.p5, span)]);
      stats.push([t("profileTerrain.stat_median"), formatValue(c.family, c.quantiles.p50, span)]);
      stats.push([t("profileTerrain.stat_p95"), formatValue(c.family, c.quantiles.p95, span)]);
    }
    if (c.max != null) stats.push([t("profileTerrain.stat_max"), c.max]);
    if (c.mean != null) stats.push([t("profileTerrain.stat_mean"), formatValue(c.family, c.mean)]);
    if (c.stddev != null)
      stats.push([
        t("profileTerrain.stat_stddev"),
        c.family === "temporal"
          ? t("profileTerrain.days", { n: (c.stddev / 86400).toFixed(1) })
          : formatValue("numeric", c.stddev),
      ]);
    if (c.skewness != null) stats.push([t("profileTerrain.stat_skewness"), c.skewness.toFixed(2)]);
    if (c.binning === "sketch")
      stats.push([
        t("profileTerrain.stat_beyond"),
        t("profileTerrain.beyondValue", {
          low: Math.round(c.beyondLow).toLocaleString(),
          high: Math.round(c.beyondHigh).toLocaleString(),
        }),
      ]);
  }
  const nonNull = c && c.rows != null && c.nullCount != null ? c.rows - c.nullCount : null;

  return (
    <div className="p3d-detail">
      {c && (
        <section data-testid="terrain-detail" aria-live="polite">
          <div className="p3d-col-head">
            <h4>{c.name}</h4>
            <span className="p3d-kind">{t(`profileTerrain.kind_${c.family}`)}</span>
          </div>
          <div className="p3d-chips">
            {c.flags.length === 0 ? (
              <span className="p3d-chip">{t("profileTerrain.noFlags")}</span>
            ) : (
              c.flags.map((f, i) => (
                <span key={i} className={`p3d-chip${f.kind === "warn" ? " is-warn" : ""}`}>
                  {t(`profileTerrain.${f.key}`, f.params)}
                </span>
              ))
            )}
          </div>
          {c.bins.length > 0 && c.ordered && <Histogram c={c} hover={hover} onHover={onHover} />}
          {c.bins.length > 0 && !c.ordered && (
            <div className="p3d-cats">
              {c.bins.slice(0, SHOWN_CATEGORIES).map((b, j) => (
                <div
                  key={j}
                  className={`p3d-cat${hover?.column === c.name && hover.bin === j ? " is-on" : ""}`}
                  onMouseEnter={() => onHover({ column: c.name, bin: j })}
                  onMouseLeave={() => onHover(null)}
                  data-testid={`terrain-cat-${j}`}
                >
                  <span className="p3d-cat-label">{b.label}</span>
                  <span>
                    <span
                      className="p3d-cat-bar"
                      style={{ width: `${((b.count / c.maxCount) * 100).toFixed(1)}%` }}
                    />
                  </span>
                  <span className="p3d-num">{nonNull ? pctText(b.count / nonNull) : ""}</span>
                </div>
              ))}
              {c.bins.length > SHOWN_CATEGORIES && (
                <span className="p3d-note">
                  {t("profileTerrain.moreBins", { count: c.bins.length - SHOWN_CATEGORIES })}
                </span>
              )}
            </div>
          )}
          <p className="p3d-note">{t(`profileTerrain.binning_${c.binning}`)}</p>
          <dl className="p3d-stats">
            {stats.map(([k, v]) => (
              <div key={k}>
                <dt>{k}</dt>
                <dd>{v}</dd>
              </div>
            ))}
          </dl>
          <div>
            <Button
              size="xs"
              variant="default"
              onClick={() => onFocus(c.name)}
              data-testid="terrain-focus"
            >
              {t("profileTerrain.zoom")}
            </Button>
          </div>
        </section>
      )}
      <section>
        <h5 className="p3d-section-title">{t("profileTerrain.columns")}</h5>
        <div className="p3d-list">
          {order.map((name) => {
            const col = columns.find((x) => x.name === name) as TerrainColumn;
            const flags = flagText(col);
            const warn = col.flags.some((f) => f.kind === "warn");
            return (
              <button
                type="button"
                key={name}
                className="p3d-row"
                aria-pressed={name === selected}
                onClick={() => onSelect(name)}
                data-testid={`terrain-row-${name}`}
              >
                <span className="p3d-row-name">
                  {name}
                  <small className={warn ? "is-warn" : undefined}>
                    {flags.length ? flags.join(" · ") : t("profileTerrain.noFlags")}
                  </small>
                </span>
                <Spark c={col} />
                <span className="p3d-num">{col.nullShare ? pctText(col.nullShare) : "—"}</span>
              </button>
            );
          })}
        </div>
      </section>
    </div>
  );
}
