// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3e1a95-4b26-4d80-9f71-5e2b8d6a0c14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Box, Button, Group, Image, Paper, Text } from "@mantine/core";
import { Download, FileDown, Network } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { toResultRedirect } from "../api/admin";
import type { ChatArtifact as Artifact } from "../hooks/useMcpChat";
import { ResultRedirectNotice } from "./ResultRedirectNotice";

/** A file's size in the reader's language: kilobytes under a megabyte, else megabytes. */
function fileSize(bytes: number, language: string): string {
  const mb = bytes / (1024 * 1024);
  const [value, unit] = mb < 1 ? [bytes / 1024, "kilobyte"] : [mb, "megabyte"];
  return new Intl.NumberFormat(language, {
    style: "unit",
    unit,
    maximumFractionDigits: 1,
  }).format(value);
}

/**
 * What Polly handed over in a reply, besides its text: a graph it drew, with a download of the
 * picture and a way to run the same statement on the Graph page; a result delivered as a file,
 * with its download; or a file she made for the person to save (REQ-2114), as a download card.
 */
export function ChatArtifact({ artifact }: { artifact: Artifact }) {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  if (artifact.kind === "download") {
    return (
      <Paper withBorder radius="sm" p="xs" mt="xs" data-testid="chat-download">
        <Group gap="xs" wrap="nowrap" justify="space-between">
          <Group gap="xs" wrap="nowrap" style={{ minWidth: 0 }}>
            <FileDown size={16} />
            <Box style={{ minWidth: 0 }}>
              <Text size="xs" fw={500} truncate data-testid="chat-download-name">
                {artifact.filename}
              </Text>
              <Text size="xs" c="dimmed">
                {fileSize(artifact.size, i18n.language)}
              </Text>
            </Box>
          </Group>
          <Button
            component="a"
            href={`data:${artifact.content_type};base64,${artifact.data}`}
            download={artifact.filename}
            size="compact-xs"
            variant="light"
            leftSection={<Download size={12} />}
            data-testid="chat-download-save"
          >
            {t("chatPanel.download")}
          </Button>
        </Group>
      </Paper>
    );
  }
  if (artifact.kind === "file") {
    return (
      <Box mt="xs">
        <ResultRedirectNotice redirect={toResultRedirect(artifact)} />
      </Box>
    );
  }
  const src = `data:image/png;base64,${artifact.png}`;
  const caption = t("chatPanel.graphCaption", {
    nodes: artifact.nodes,
    relationships: artifact.relationships,
  });
  return (
    <Box data-testid="chat-graph" mt="xs">
      <Image src={src} alt={caption} radius="sm" bd="1px solid var(--mantine-color-default-border)" />
      <Text size="xs" c="dimmed" mt={4}>
        {caption}
      </Text>
      <Group gap="xs" mt={4}>
        <Button
          component="a"
          href={src}
          download="graph.png"
          size="compact-xs"
          variant="light"
          leftSection={<Download size={12} />}
          data-testid="chat-graph-download"
        >
          {t("chatPanel.downloadPng")}
        </Button>
        <Button
          size="compact-xs"
          variant="light"
          leftSection={<Network size={12} />}
          onClick={() => navigate("/graph", { state: { query: artifact.cypher } })}
          data-testid="chat-graph-open"
        >
          {t("chatPanel.openOnGraphPage")}
        </Button>
      </Group>
    </Box>
  );
}
