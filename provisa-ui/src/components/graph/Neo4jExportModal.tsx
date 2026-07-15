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
import { Alert, Button, Group, Modal, PasswordInput, Stack, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { GNode, GEdge } from "./graph-model";
import { buildCypherScript, exportToNeo4j, downloadBlob } from "./graph-export";
import type { Neo4jConnection } from "./graph-export";

interface Neo4jExportModalProps {
  nodes: GNode[];
  edges: GEdge[];
  onClose: () => void;
}

export function Neo4jExportModal({ nodes, edges, onClose }: Neo4jExportModalProps) {
  const { t } = useTranslation();
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
      const errSuffix = result.errors.length
        ? t("neo4jExportModal.errorsSuffix", { count: result.errors.length, first: result.errors[0] })
        : "";
      setMessage(`${t("neo4jExportModal.statementsImported", { count: result.imported })}${errSuffix}`);
    } catch (e) {
      setStatus("error");
      setMessage(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("neo4jExportModal.title")}
      centered
      data-testid="neo4j-export-modal"
    >
      <Stack gap="md">
        <Text size="sm" c="dimmed" data-testid="neo4j-export-summary">
          {t("neo4jExportModal.nodeCount", { count: nodes.length })} ·{" "}
          {t("neo4jExportModal.edgeCount", { count: edges.length })}
        </Text>

        <Button variant="default" onClick={handleDownloadScript} data-testid="neo4j-download-script-btn">
          {t("neo4jExportModal.downloadScript")}
        </Button>

        <Text size="sm" c="dimmed" ta="center">
          {t("neo4jExportModal.divider")}
        </Text>

        <TextInput
          label={t("neo4jExportModal.httpUrl")}
          value={url}
          onChange={(e) => setUrl(e.currentTarget.value)}
          placeholder="http://localhost:7474"
          data-testid="neo4j-url-input"
        />
        <TextInput
          label={t("neo4jExportModal.database")}
          value={database}
          onChange={(e) => setDatabase(e.currentTarget.value)}
          placeholder="neo4j"
          data-testid="neo4j-database-input"
        />
        <TextInput
          label={t("neo4jExportModal.username")}
          value={username}
          onChange={(e) => setUsername(e.currentTarget.value)}
          data-testid="neo4j-username-input"
        />
        <PasswordInput
          label={t("neo4jExportModal.password")}
          value={password}
          onChange={(e) => setPassword(e.currentTarget.value)}
          data-testid="neo4j-password-input"
        />

        {message && (
          <Alert
            color={status === "error" ? "red" : status === "done" ? "green" : "blue"}
            data-testid="neo4j-export-message"
          >
            {message}
          </Alert>
        )}

        <Group justify="flex-end">
          <Button variant="default" onClick={onClose} data-testid="neo4j-close-btn">
            {t("neo4jExportModal.close")}
          </Button>
          <Button
            onClick={handleExportToServer}
            loading={status === "exporting"}
            data-testid="neo4j-export-btn"
          >
            {status === "exporting" ? t("neo4jExportModal.exporting") : t("neo4jExportModal.exportToServer")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
