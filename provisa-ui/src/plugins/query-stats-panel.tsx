// Copyright (c) 2026 Kenneth Stott
// Canary: 8a9b0c1d-2e3f-4a5b-6c7d-8e9f0a1b2c3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { useState, useEffect } from "react";
import { subscribeQueryStats, type QueryStats } from "../query-stats";

export function QueryStatsPanel() {
  const [stats, setStats] = useState<QueryStats | null>(null);
  useEffect(() => subscribeQueryStats(setStats), []);

  if (!stats) return null;

  return (
    <div className="query-stats-panel">
      <div className="query-stats-panel-header">
        Query Stats — {stats.total_elapsed_ms} ms total
      </div>
      <table className="query-stats-panel-table">
        <thead>
          <tr>
            <th>field</th>
            <th>source</th>
            <th>strategy</th>
            <th>ms</th>
            <th>rows</th>
            <th>cache</th>
          </tr>
        </thead>
        <tbody>
          {stats.sources.map((s, i) => (
            <tr key={i}>
              <td>{s.field}</td>
              <td>{s.source}</td>
              <td>{s.strategy}</td>
              <td className="stats-num">{s.elapsed_ms}</td>
              <td className="stats-num">{s.rows}</td>
              <td>{s.cache_hit ? "✓" : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
