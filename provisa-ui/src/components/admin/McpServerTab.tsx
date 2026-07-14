// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { fetchMcpServer, type McpServerStatus } from "../../api/admin";

// REQ-1008: read-only status of the in-process MCP server. It is enabled purely via the
// PROVISA_MCP_PORT env var at boot, so this tab reports current state + how to enable it rather
// than offering a control that could not actually toggle the running server.
export function McpServerTab() {
  const [status, setStatus] = useState<McpServerStatus | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchMcpServer()
      .then(setStatus)
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <div className="error-banner">{error}</div>;
  if (!status) return <div>Loading…</div>;

  return (
    <div className="mcp-server-tab" style={{ maxWidth: 720 }}>
      <p className="muted">
        The Model Context Protocol (MCP) server exposes governed catalog discovery and SQL
        execution to external AI agents. Every call is governed by the caller's role — the server
        never defaults to admin.
      </p>

      <div className="form-card" style={{ display: "grid", gap: "0.5rem" }}>
        <div>
          Status:{" "}
          <strong
            data-testid="mcp-status"
            style={{ color: status.enabled ? "var(--success)" : "var(--text-muted)" }}
          >
            {status.enabled ? "Enabled" : "Disabled"}
          </strong>
        </div>

        {status.enabled ? (
          <>
            <div>
              Endpoint:{" "}
              <code data-testid="mcp-endpoint">http://0.0.0.0:{status.port}</code>
            </div>
            <div>
              Transport: <code>{status.transport}</code>
            </div>
            <div>
              Bound stdio role:{" "}
              {status.stdio_role ? (
                <code data-testid="mcp-role">{status.stdio_role}</code>
              ) : (
                <span className="muted">
                  none set — stdio calls require <code>{status.role_env_var}</code>
                </span>
              )}
            </div>
            <div>
              Row ceiling (run_sql): <code>{status.max_rows}</code>
            </div>
          </>
        ) : (
          <div
            className="warn-banner"
            data-testid="mcp-enable-hint"
            style={{ padding: "0.5rem 0.75rem", border: "1px solid #b8860b", borderRadius: 4 }}
          >
            The MCP server is opt-in. Set the <code>{status.enable_env_var}</code> environment
            variable to a port and restart the service to enable it. For the local stdio transport,
            also set <code>{status.role_env_var}</code> to a provisa role.
          </div>
        )}
      </div>

      <h4 style={{ marginTop: "1.5rem" }}>Exposed Tools</h4>
      <table className="data-table" data-testid="mcp-tools">
        <thead>
          <tr>
            <th>Tool</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {status.tools.map((t) => (
            <tr key={t.name}>
              <td style={{ fontFamily: "monospace" }}>{t.name}</td>
              <td>{t.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
