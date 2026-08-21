// Copyright (c) 2026 Kenneth Stott
// Canary: 11557514-6a05-4a67-ae76-7227c880597f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef } from "react";
import "./MermaidDiagram.css";

let _mermaidDagSeq = 0;

/** Renders the query-stats DAG (provisa_stats.mermaid) — shared by the GraphQL and SQL surfaces. */
export function MermaidDiagram({ chart }: { chart: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const idPrefixRef = useRef(`mermaid-dag-${++_mermaidDagSeq}`);
  useEffect(() => {
    let cancelled = false;
    const charts = chart.split(/\n\n(?=flowchart)/).filter(Boolean);
    import("mermaid").then((m) => {
      if (cancelled || !ref.current) return;
      m.default.initialize({ startOnLoad: false, theme: "dark" });
      const renders = charts.map((c, i) =>
        m.default.render(`${idPrefixRef.current}-${i}`, c).then(({ svg }) => svg),
      );
      Promise.all(renders).then((svgs) => {
        if (!cancelled && ref.current) ref.current.innerHTML = svgs.join("");
      });
    });
    return () => {
      cancelled = true;
    };
  }, [chart]);
  return <div ref={ref} className="stats-mermaid" />;
}
