// Copyright (c) 2026 Kenneth Stott
// Canary: c07134ea-2571-4321-abd6-3927c3d7e322
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: the SPARQL tab's export, where the Cypher tab's is the Neo4j export. A frame's answer
// is written in its own form's formats (sparqlExport.ts): a graph as RDF, rows as SPARQL results.

import { useState } from "react";
import { Alert, Button, Group, Modal, SegmentedControl, Select, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { downloadBlob } from "./graph-export";
import { fetchSparqlExport, SPARQL_EXPORT_FORMATS, type SparqlExportable } from "./sparqlExport";

interface SparqlExportModalProps {
  /** The frames' answers, newest first. */
  frames: SparqlExportable[];
  roleHeader: string | null | undefined;
  onClose: () => void;
}

export function SparqlExportModal({ frames, roleHeader, onClose }: SparqlExportModalProps) {
  const { t } = useTranslation();
  const [frameId, setFrameId] = useState<string | null>(frames[0]?.id ?? null);
  const frame = frames.find((f) => f.id === frameId) ?? null;
  const formats = frame ? SPARQL_EXPORT_FORMATS[frame.form] : [];
  const [formatKey, setFormatKey] = useState<string | null>(formats[0]?.key ?? null);
  // A frame of another form has formats of its own: its default until one is chosen.
  const format = formats.find((f) => f.key === formatKey) ?? formats[0];
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const handleDownload = async () => {
    if (!frame) return;
    setBusy(true);
    setError("");
    try {
      const blob = await fetchSparqlExport(frame.query, format.media, roleHeader);
      downloadBlob(blob, `sparql-result.${format.ext}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("sparqlExportModal.title")}
      centered
      data-testid="sparql-export-modal"
    >
      <Stack gap="md">
        {!frame ? (
          <Text size="sm" c="dimmed" data-testid="sparql-export-empty">
            {t("sparqlExportModal.empty")}
          </Text>
        ) : (
          <>
            {frames.length > 1 && (
              <Select
                label={t("sparqlExportModal.result")}
                data={frames.map((f) => ({ value: f.id, label: f.query }))}
                value={frame.id}
                onChange={(v) => {
                  setFrameId(v);
                  setFormatKey(null);
                  setError("");
                }}
                allowDeselect={false}
                data-testid="sparql-export-frame"
              />
            )}
            <Stack gap={4}>
              <Text size="sm" fw={500}>
                {t("sparqlExportModal.format")}
              </Text>
              <SegmentedControl
                value={format.key}
                onChange={(v) => {
                  setFormatKey(v);
                  setError("");
                }}
                data={formats.map((f) => ({
                  value: f.key,
                  label: t(`sparqlExportModal.formats.${f.key}`),
                }))}
                data-testid="sparql-export-format"
              />
            </Stack>
            {error && (
              <Alert color="red" data-testid="sparql-export-error">
                {error}
              </Alert>
            )}
          </>
        )}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose} data-testid="sparql-export-close">
            {t("sparqlExportModal.close")}
          </Button>
          {frame && (
            <Button onClick={handleDownload} loading={busy} data-testid="sparql-export-download">
              {t("sparqlExportModal.download")}
            </Button>
          )}
        </Group>
      </Stack>
    </Modal>
  );
}
