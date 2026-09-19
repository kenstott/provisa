// Copyright (c) 2026 Kenneth Stott
// Canary: 84b1a4de-cd1c-487c-9913-a0cea292cfce
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Group,
  Modal,
  PasswordInput,
  Stack,
  Text,
  Tooltip,
} from "@mantine/core";
import { Pencil, Trash2, ArrowRight, RefreshCw } from "lucide-react";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import type { Source } from "../../types/admin";
import { SOURCE_TYPES } from "./constants";
import { uiType } from "./sourceHelpers";
import { useRefreshKaggleSource } from "../../hooks/useAdminQueries";

const API_TYPES = new Set(["graphql_remote", "grpc_remote", "openapi"]);
const PATH_TYPES = new Set(["sqlite", "csv", "parquet", "files"]);

interface SourceDetailPanelProps {
  s: Source;
  domainsEnabled: boolean;
  getEffectiveTtl: (source: Source) => string;
  onEdit: () => void;
  onNavigate: () => void;
  onDelete: () => void;
}

export function SourceDetailPanel({
  s,
  domainsEnabled,
  getEffectiveTtl,
  onEdit,
  onNavigate,
  onDelete,
}: SourceDetailPanelProps) {
  const { t } = useTranslation();
  const { refreshKaggleSource, loading: refreshing } = useRefreshKaggleSource();
  const [refreshModalOpen, setRefreshModalOpen] = useState(false);
  const [refreshToken, setRefreshToken] = useState("");
  const [refreshError, setRefreshError] = useState<string | null>(null);

  const isApiType = API_TYPES.has(s.type);
  const isPathType = PATH_TYPES.has(s.type);
  // REQ-1780/1781/1782/1783: kaggle_owner/kaggle_ref are stashed in federation_hints at creation
  // (KaggleFormSection.tsx) -- no other Source field records a Kaggle origin, so this is the only
  // way to know a "Refresh from Kaggle" action applies to this row.
  let kaggleOrigin: { owner: string; ref: string } | null = null;
  if (s.federationHintsJson) {
    try {
      const hints = JSON.parse(s.federationHintsJson) as Record<string, string>;
      if (hints.kaggle_owner && hints.kaggle_ref) {
        kaggleOrigin = { owner: hints.kaggle_owner, ref: hints.kaggle_ref };
      }
    } catch {
      // Not JSON, or no kaggle_* keys -- not a Kaggle-derived source.
    }
  }

  const handleRefresh = async () => {
    setRefreshError(null);
    const result = await refreshKaggleSource(s.id, refreshToken);
    if (result.success) {
      setRefreshModalOpen(false);
      setRefreshToken("");
    } else {
      setRefreshError(result.message);
    }
  };

  const rows: [string, string | number][] = [
    ["description", s.description || "—"],
    ["type", SOURCE_TYPES.find((ty) => ty.value === uiType(s.type))?.label ?? s.type],
    ...(isApiType
      ? ([["endpoint", s.path || "—"]] as [string, string | number][])
      : isPathType
        ? ([["path", s.path || "—"]] as [string, string | number][])
        : ([
            ["host", s.host || "—"],
            ["port", s.port || "—"],
            ["database", s.database || "—"],
            ["username", s.username || "—"],
          ] as [string, string | number][])),
    ["naming", s.gqlNamingConvention || t("sourceDetailPanel.namingInherit")],
    [
      "cache",
      s.cacheEnabled ? t("sourceDetailPanel.cacheEnabled") : t("sourceDetailPanel.cacheDisabled"),
    ],
    ["cacheTtl", s.cacheTtl != null ? `${s.cacheTtl}s` : t("sourceDetailPanel.ttlInherit")],
    ["effectiveTtl", getEffectiveTtl(s)],
    [
      "allowedDomains",
      (s.allowedDomains ?? []).length
        ? (s.allowedDomains ?? []).join(", ")
        : t("sourceDetailPanel.domainsUnrestricted"),
    ],
  ];

  return (
    <Stack gap="sm">
      <dl
        style={{
          display: "grid",
          gridTemplateColumns: "max-content 1fr",
          gap: "0.25rem 1rem",
          margin: 0,
          color: "var(--text)",
        }}
      >
        {rows
          .filter(([k]) => domainsEnabled || k !== "allowedDomains")
          .map(([k, v]) => (
            <React.Fragment key={k}>
              <Text component="dt" c="dimmed" fw={500} size="sm">
                {t(`sourceDetailPanel.field.${k}`)}
              </Text>
              <Text component="dd" m={0} size="sm">
                {v}
              </Text>
            </React.Fragment>
          ))}
      </dl>
      <Group gap="xs" mt={4}>
        <Tooltip label={t("sourceDetailPanel.editTitle")}>
          <ActionIcon
            variant="subtle"
            aria-label={t("sourceDetailPanel.editTitle")}
            data-testid="source-detail-edit"
            onClick={(e) => {
              e.stopPropagation();
              onEdit();
            }}
          >
            <Pencil size={14} />
          </ActionIcon>
        </Tooltip>
        {s.id !== "provisa-otel" && (
          <Tooltip label={t("sourceDetailPanel.navigateTitle")}>
            <ActionIcon
              variant="subtle"
              aria-label={t("sourceDetailPanel.navigateTitle")}
              data-testid="source-detail-navigate"
              onClick={(e) => {
                e.stopPropagation();
                onNavigate();
              }}
            >
              <ArrowRight size={14} />
            </ActionIcon>
          </Tooltip>
        )}
        {kaggleOrigin && (
          <Tooltip label={t("sourceDetailPanel.refreshKaggleTitle")}>
            <ActionIcon
              variant="subtle"
              aria-label={t("sourceDetailPanel.refreshKaggleTitle")}
              data-testid="source-detail-refresh-kaggle"
              onClick={(e) => {
                e.stopPropagation();
                setRefreshError(null);
                setRefreshModalOpen(true);
              }}
            >
              <RefreshCw size={14} />
            </ActionIcon>
          </Tooltip>
        )}
        <ConfirmDialog
          title={t("sourceDetailPanel.deleteTitle", { id: s.id })}
          consequence={t("sourceDetailPanel.deleteConsequence", { id: s.id })}
          onConfirm={onDelete}
        >
          {(open) => (
            <Tooltip label={t("sourceDetailPanel.deleteTitleShort")}>
              <ActionIcon
                variant="subtle"
                color="red"
                aria-label={t("sourceDetailPanel.deleteTitleShort")}
                data-testid="source-detail-delete"
                onClick={(e) => {
                  e.stopPropagation();
                  open();
                }}
              >
                <Trash2 size={14} />
              </ActionIcon>
            </Tooltip>
          )}
        </ConfirmDialog>
      </Group>
      {kaggleOrigin && (
        <Modal
          opened={refreshModalOpen}
          onClose={() => setRefreshModalOpen(false)}
          title={t("sourceDetailPanel.refreshKaggleTitle")}
        >
          <Stack gap="sm">
            <Text size="sm" c="dimmed">
              {t("sourceDetailPanel.refreshKaggleHint", {
                owner: kaggleOrigin.owner,
                ref: kaggleOrigin.ref,
              })}
            </Text>
            <PasswordInput
              label={t("sourceDetailPanel.kaggleTokenLabel")}
              value={refreshToken}
              onChange={(e) => setRefreshToken(e.currentTarget.value)}
              data-testid="source-detail-refresh-kaggle-token"
            />
            {refreshError && (
              <Alert color="red" variant="light" py={4} px="sm">
                {refreshError}
              </Alert>
            )}
            <Group justify="flex-end">
              <Button
                type="button"
                loading={refreshing}
                disabled={!refreshToken}
                onClick={handleRefresh}
                data-testid="source-detail-refresh-kaggle-submit"
              >
                {t("sourceDetailPanel.refreshKaggleSubmit")}
              </Button>
            </Group>
          </Stack>
        </Modal>
      )}
    </Stack>
  );
}
