// Copyright (c) 2026 Kenneth Stott
// Canary: ce243a88-d4cf-41a3-99e0-741e6acd3802
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import {
  useScheduledTasks,
  useToggleScheduledTask,
  useCreateScheduledTask,
} from "../../hooks/useAdminQueries";
import { fetchActions, type TrackedWebhook } from "../../api/actions";

const PAGE_SIZE = 50;

export function ScheduledTasks() {
  const { scheduledTasks: tasks, loading } = useScheduledTasks();
  const { toggleScheduledTask } = useToggleScheduledTask();
  const { createScheduledTask } = useCreateScheduledTask();
  const [toggling, setToggling] = useState<string | null>(null);
  const [taskPage, setTaskPage] = useState(0);

  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  useEffect(() => {
    fetchActions().then((a) => setWebhooks(a.webhooks)).catch(() => {});
  }, []);

  const [showForm, setShowForm] = useState(false);
  const [newId, setNewId] = useState("");
  const [newName, setNewName] = useState("");
  const [newCron, setNewCron] = useState("");
  const [newWebhookName, setNewWebhookName] = useState("");
  const [argValues, setArgValues] = useState<Record<string, string>>({});
  const [formMsg, setFormMsg] = useState("");
  const [creating, setCreating] = useState(false);

  const selectedWebhook = webhooks.find((w) => w.name === newWebhookName) ?? null;

  const handleToggle = async (id: string, enabled: boolean) => {
    setToggling(id);
    await toggleScheduledTask(id, enabled);
    setToggling(null);
  };

  const handleCreate = async () => {
    if (!newId.trim() || !newName.trim() || !newCron.trim() || !newWebhookName) {
      setFormMsg("ID, Name, Cron, and Webhook are required.");
      return;
    }
    setCreating(true);
    setFormMsg("");
    const argsJson = Object.keys(argValues).length
      ? JSON.stringify(argValues)
      : undefined;
    const result = await createScheduledTask(
      newId.trim(),
      newName.trim(),
      newCron.trim(),
      newWebhookName,
      argsJson,
    );
    setCreating(false);
    if (result.success) {
      setShowForm(false);
      setNewId("");
      setNewName("");
      setNewCron("");
      setNewWebhookName("");
      setArgValues({});
    } else {
      setFormMsg(result.message);
    }
  };

  if (loading) return <p>Loading scheduled tasks...</p>;

  const totalPages = Math.max(1, Math.ceil(tasks.length / PAGE_SIZE));
  const paged = tasks.slice(taskPage * PAGE_SIZE, (taskPage + 1) * PAGE_SIZE);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "0.5rem" }}>
        <button onClick={() => { setShowForm((v) => !v); setFormMsg(""); }}>
          {showForm ? "✕" : "+ Scheduled Task"}
        </button>
      </div>

      {showForm && (
        <div className="form-card" style={{ marginBottom: "1rem" }}>
          <label>
            ID
            <input value={newId} onChange={(e) => setNewId(e.target.value)} placeholder="my-task" />
          </label>
          <label>
            Name
            <input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="My Task" />
          </label>
          <label>
            Cron Expression
            <input value={newCron} onChange={(e) => setNewCron(e.target.value)} placeholder="0 * * * *" />
          </label>
          <label>
            Webhook
            <select
              value={newWebhookName}
              onChange={(e) => {
                setNewWebhookName(e.target.value);
                setArgValues({});
              }}
            >
              <option value="">Select webhook…</option>
              {webhooks.map((w) => (
                <option key={w.name} value={w.name}>{w.name}</option>
              ))}
            </select>
          </label>
          {selectedWebhook?.arguments.map((arg) => (
            <label key={arg.name}>
              {arg.name} <span style={{ color: "var(--text-muted)", fontSize: "0.8em" }}>({arg.type})</span>
              <input
                value={argValues[arg.name] ?? ""}
                onChange={(e) =>
                  setArgValues((prev) => ({ ...prev, [arg.name]: e.target.value }))
                }
                placeholder={arg.type}
              />
            </label>
          ))}
          {formMsg && <p style={{ color: "var(--error)" }}>{formMsg}</p>}
          <div style={{ display: "flex", gap: "0.5rem" }}>
            <button onClick={handleCreate} disabled={creating}>
              {creating ? "Creating..." : "+ Scheduled Task"}
            </button>
          </div>
        </div>
      )}

      {tasks.length === 0 ? (
        <p>No scheduled tasks configured.</p>
      ) : (
        <>
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
              {paged.map((task) => (
                <tr key={task.id}>
                  <td>
                    <code>{task.id}</code>
                  </td>
                  <td>{task.name}</td>
                  <td>
                    <code>{task.cronExpression}</code>
                  </td>
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
          {totalPages > 1 && (
            <div
              style={{
                display: "flex",
                gap: "0.5rem",
                alignItems: "center",
                justifyContent: "flex-end",
                padding: "0.5rem 0",
              }}
            >
              <button onClick={() => setTaskPage(0)} disabled={taskPage === 0}>
                «
              </button>
              <button onClick={() => setTaskPage((p) => p - 1)} disabled={taskPage === 0}>
                ‹
              </button>
              <span>
                Page {taskPage + 1} / {totalPages}
              </span>
              <button onClick={() => setTaskPage((p) => p + 1)} disabled={taskPage >= totalPages - 1}>
                ›
              </button>
              <button onClick={() => setTaskPage(totalPages - 1)} disabled={taskPage >= totalPages - 1}>
                »
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
