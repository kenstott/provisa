// Copyright (c) 2025 Kenneth Stott
// Canary: ce243a88-d4cf-41a3-99e0-741e6acd3802
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { fetchScheduledTasks, toggleScheduledTask } from "../../api/admin";
import type { ScheduledTask } from "../../api/admin";

export function ScheduledTasks() {
  const [tasks, setTasks] = useState<ScheduledTask[]>([]);
  const [loading, setLoading] = useState(true);
  const [toggling, setToggling] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    fetchScheduledTasks().then(setTasks).finally(() => setLoading(false));
  };

  useEffect(load, []);

  const handleToggle = async (id: string, enabled: boolean) => {
    setToggling(id);
    await toggleScheduledTask(id, enabled);
    load();
    setToggling(null);
  };

  if (loading) return <p>Loading scheduled tasks...</p>;
  if (tasks.length === 0) return <p>No scheduled tasks configured.</p>;

  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Name</th>
          <th>Cron Expression</th>
          <th>Webhook URL</th>
          <th>Enabled</th>
          <th>Last Run</th>
          <th>Next Run</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {tasks.map((task) => (
          <tr key={task.id}>
            <td><code>{task.id}</code></td>
            <td>{task.name}</td>
            <td><code>{task.cronExpression}</code></td>
            <td className="reasoning-cell" style={{ maxWidth: 250 }}>
              {task.webhookUrl || "—"}
            </td>
            <td>
              <span className={`status-badge status-${task.enabled ? "active" : "disabled"}`}>
                {task.enabled ? "enabled" : "disabled"}
              </span>
            </td>
            <td>{task.lastRunAt ? new Date(task.lastRunAt).toLocaleString() : "never"}</td>
            <td>{task.nextRunAt ? new Date(task.nextRunAt).toLocaleString() : "—"}</td>
            <td>
              <button
                onClick={() => handleToggle(task.id, !task.enabled)}
                disabled={toggling === task.id}
                style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
              >
                {toggling === task.id ? "..." : task.enabled ? "Disable" : "Enable"}
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
