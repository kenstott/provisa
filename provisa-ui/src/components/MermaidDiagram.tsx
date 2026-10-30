// Copyright (c) 2026 Kenneth Stott
// Canary: 11557514-6a05-4a67-ae76-7227c880597f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { fitSvgToWidth } from "./mermaidFit";
import "./MermaidDiagram.css";

let _mermaidDagSeq = 0;

/** Renders the query-stats DAG (provisa_stats.mermaid) — shared by the GraphQL and SQL surfaces.
 *
 * Every chart is parsed before any is drawn. A chart Mermaid cannot read is reported in the
 * diagram's own place, with Mermaid's message, instead of Mermaid's error graphic (which it
 * appends to the page body, outside this component). */
export function MermaidDiagram({ chart }: { chart: string }) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const idPrefixRef = useRef(`mermaid-dag-${++_mermaidDagSeq}`);
  // Keyed by the chart it belongs to, so a new chart never shows the last one's error.
  const [failure, setFailure] = useState<{ chart: string; message: string } | null>(null);
  const error = failure?.chart === chart ? failure.message : null;
  useEffect(() => {
    let cancelled = false;
    const charts = chart.split(/\n\n(?=flowchart)/).filter(Boolean);
    const ids = charts.map((_, i) => `${idPrefixRef.current}-${i}`);
    import("mermaid").then(async (m) => {
      if (cancelled || !ref.current) return;
      const mermaid = m.default;
      // suppressErrorRendering: a failed render throws and removes its scratch element instead
      // of drawing Mermaid's error graphic on the page body.
      mermaid.initialize({ startOnLoad: false, theme: "dark", suppressErrorRendering: true });
      try {
        for (const c of charts) await mermaid.parse(c);
        const svgs = await Promise.all(
          charts.map((c, i) => mermaid.render(ids[i], c).then(({ svg }) => svg)),
        );
        if (!cancelled && ref.current) {
          ref.current.innerHTML = svgs.join("");
          ref.current.querySelectorAll("svg").forEach(fitSvgToWidth);
        }
      } catch (e) {
        if (cancelled) return;
        if (ref.current) ref.current.innerHTML = "";
        setFailure({ chart, message: e instanceof Error ? e.message : String(e) });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [chart]);
  return (
    <div className="stats-mermaid">
      {error !== null && (
        <div className="stats-mermaid-error" role="alert" data-testid="mermaid-error">
          <div className="stats-mermaid-error-title">{t("mermaidDiagram.unavailable")}</div>
          <pre className="stats-mermaid-error-message">{error}</pre>
        </div>
      )}
      <div ref={ref} />
    </div>
  );
}
