// Copyright (c) 2026 Kenneth Stott
// Canary: 3f7b2a91-e4c8-4d2b-9f5e-1a6c3d8e7f04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { GNode, GEdge } from "./graph-model";
import { buildCypherScript, exportToNeo4j, downloadBlob } from "./graph-export";
import type { Neo4jConnection } from "./graph-export";

interface Neo4jExportModalProps {
  nodes: GNode[];
  edges: GEdge[];
  onClose: () => void;
}

export function Neo4jExportModal({ nodes, edges, onClose }: Neo4jExportModalProps) {
  const [url, setUrl] = useState("http://localhost:7474");
  const [database, setDatabase] = useState("neo4j");
  const [username, setUsername] = useState("neo4j");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState<"idle" | "exporting" | "done" | "error">("idle");
  const [message, setMessage] = useState("");

  const handleDownloadScript = () => {
    const script = buildCypherScript(nodes, edges);
    downloadBlob(new Blob([script], { type: "text/plain" }), "graph.cypher");
  };

  const handleExportToServer = async () => {
    setStatus("exporting");
    setMessage("");
    try {
      const conn: Neo4jConnection = { url, username, password, database };
      const result = await exportToNeo4j(nodes, edges, conn);
      setStatus("done");
      const errSuffix = result.errors.length ? ` (${result.errors.length} error${result.errors.length > 1 ? "s" : ""}: ${result.errors[0]})` : "";
      setMessage(`${result.imported} statements imported.${errSuffix}`);
    } catch (e) {
      setStatus("error");
      setMessage(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="nf-modal-backdrop" onClick={onClose}>
      <div className="neo4j-modal" onClick={(e) => e.stopPropagation()}>
        <div className="nf-modal-title">Neo4j Export</div>
        <div className="neo4j-modal-summary">
          {nodes.length} node{nodes.length !== 1 ? "s" : ""} · {edges.length} relationship{edges.length !== 1 ? "s" : ""}
        </div>

        <button className="neo4j-script-btn" onClick={handleDownloadScript}>
          Download Cypher Script (.cypher)
        </button>

        <div className="neo4j-modal-divider">— or export directly to a server —</div>

        <div className="nf-modal-field">
          <label className="nf-modal-label">HTTP URL</label>
          <input
            className="nf-modal-input"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="http://localhost:7474"
          />
        </div>
        <div className="nf-modal-field">
          <label className="nf-modal-label">Database</label>
          <input
            className="nf-modal-input"
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            placeholder="neo4j"
          />
        </div>
        <div className="nf-modal-field">
          <label className="nf-modal-label">Username</label>
          <input
            className="nf-modal-input"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
        </div>
        <div className="nf-modal-field">
          <label className="nf-modal-label">Password</label>
          <input
            className="nf-modal-input"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </div>

        {message && (
          <div className={`neo4j-export-msg neo4j-export-msg--${status}`}>{message}</div>
        )}

        <div className="nf-modal-actions">
          <button className="nf-modal-cancel" onClick={onClose}>
            Close
          </button>
          <button
            className="nf-modal-run"
            onClick={handleExportToServer}
            disabled={status === "exporting"}
          >
            {status === "exporting" ? "Exporting…" : "Export to Server"}
          </button>
        </div>
      </div>
    </div>
  );
}
