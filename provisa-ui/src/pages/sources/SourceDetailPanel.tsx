// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Stack, Text, Tooltip } from "@mantine/core";
import { Pencil, Trash2, ArrowRight } from "lucide-react";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import type { Source } from "../../types/admin";
import { SOURCE_TYPES } from "./constants";

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

  const rows: [string, string | number][] = [
    ["description", s.description || "—"],
    ["type", SOURCE_TYPES.find((ty) => ty.value === s.type)?.label ?? s.type],
    ["host", s.host || "—"],
    ["port", s.port || "—"],
    ["database", s.database || "—"],
    ["username", s.username || "—"],
    ["naming", s.gqlNamingConvention || t("sourceDetailPanel.namingInherit")],
    ["cache", s.cacheEnabled ? t("sourceDetailPanel.cacheEnabled") : t("sourceDetailPanel.cacheDisabled")],
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
    </Stack>
  );
}
