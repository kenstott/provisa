// Copyright (c) 2026 Kenneth Stott
// Canary: 84b1a4de-cd1c-487c-9913-a0cea292cfce
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { PanelActions } from "../../components/list/SplitPane";
import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Button, Group, Modal, Stack, Text } from "@mantine/core";
import { Pencil, Trash2, ArrowRight, RefreshCw, Play } from "lucide-react";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import type { Source } from "../../types/admin";
import { useRefreshKaggleSource } from "../../hooks/useAdminQueries";
import { runProfiler, type SourceRunOutcome } from "../../api/profiler";
import { FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import { Tip } from "../../components/Tip";

interface SourceDetailPanelProps {
  s: Source;
  /** The source's settings read: its edit form drawn read-only; null until they are read. */
  fields: ReactNode;
  onEdit: () => void;
  onNavigate: () => void;
  onDelete: () => void;
}

export function SourceDetailPanel({
  s,
  fields,
  onEdit,
  onNavigate,
  onDelete,
}: SourceDetailPanelProps) {
  const { t } = useTranslation();
  const { refreshKaggleSource, loading: refreshing } = useRefreshKaggleSource();
  const [refreshModalOpen, setRefreshModalOpen] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);

  // REQ-1934: a Data Profiler source is a schedule; it can also be run at once for all its members.
  const isProfiler = s.type === "data_profiler";
  const [profiling, setProfiling] = useState(false);
  const [profileOutcomes, setProfileOutcomes] = useState<SourceRunOutcome[] | null>(null);
  const [profileError, setProfileError] = useState<string | null>(null);
  const handleRunProfiler = async () => {
    setProfiling(true);
    setProfileOutcomes(null);
    setProfileError(null);
    try {
      setProfileOutcomes(await runProfiler(s.id));
    } catch (e: unknown) {
      setProfileError(e instanceof Error ? e.message : String(e));
    } finally {
      setProfiling(false);
    }
  };
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
    const result = await refreshKaggleSource(s.id);
    if (result.success) {
      setRefreshModalOpen(false);
    } else {
      setRefreshError(result.message);
    }
  };

  return (
    <Stack gap="sm">
      {fields}
      {/* In a panel these sit in its pinned head, beside the source's name. */}
      <PanelActions>
        <Group gap="xs" wrap="nowrap">
          <Tip text={t("sourceDetailPanel.editTitle")} named>
            <ActionIcon
              aria-label={t("sourceDetailPanel.editTitle")}
              data-testid="source-detail-edit"
              onClick={(e) => {
                e.stopPropagation();
                onEdit();
              }}
            >
              <Pencil size={14} />
            </ActionIcon>
          </Tip>
          {s.id !== "provisa-otel" && (
            <Tip text={t("sourceDetailPanel.navigateTitle")} named>
              <ActionIcon
                aria-label={t("sourceDetailPanel.navigateTitle")}
                data-testid="source-detail-navigate"
                onClick={(e) => {
                  e.stopPropagation();
                  onNavigate();
                }}
              >
                <ArrowRight size={14} />
              </ActionIcon>
            </Tip>
          )}
          {kaggleOrigin && (
            <Tip text={t("sourceDetailPanel.refreshKaggleTitle")} named>
              <ActionIcon
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
            </Tip>
          )}
          {isProfiler && (
            // REQ-1934: an ad hoc run of every member table, beside the source's other actions.
            <Tip text={t("sourceDetailPanel.runProfilerTitle")} named>
              <ActionIcon
                aria-label={t("sourceDetailPanel.runProfilerTitle")}
                data-testid="source-detail-run-profiler"
                loading={profiling}
                onClick={(e) => {
                  e.stopPropagation();
                  void handleRunProfiler();
                }}
              >
                <Play size={14} />
              </ActionIcon>
            </Tip>
          )}
          <ConfirmDialog
            title={t("sourceDetailPanel.deleteTitle", { id: s.id })}
            consequence={t("sourceDetailPanel.deleteConsequence", { id: s.id })}
            onConfirm={onDelete}
          >
            {(open) => (
              <Tip text={t("sourceDetailPanel.deleteTitleShort")} named>
                <ActionIcon
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
              </Tip>
            )}
          </ConfirmDialog>
        </Group>
      </PanelActions>
      {kaggleOrigin && (
        <Modal
          opened={refreshModalOpen}
          onClose={() => setRefreshModalOpen(false)}
          title={t("sourceDetailPanel.refreshKaggleTitle")}
        >
          <FormLayout>
            <FormField size="full">
              <Text size="sm" c="dimmed">
                {t("sourceDetailPanel.refreshKaggleHint", {
                  owner: kaggleOrigin.owner,
                  ref: kaggleOrigin.ref,
                })}
              </Text>
            </FormField>
            {refreshError && (
              <FormField size="full">
                <Alert color="red" variant="light" py={4} px="sm">
                  {refreshError}
                </Alert>
              </FormField>
            )}
            {/* Refreshing is not a save: the button keeps its word, at the start. */}
            <FormFooter
              action={
                <Button
                  type="button"
                  loading={refreshing}
                  onClick={handleRefresh}
                  data-testid="source-detail-refresh-kaggle-submit"
                >
                  {t("sourceDetailPanel.refreshKaggleSubmit")}
                </Button>
              }
            />
          </FormLayout>
        </Modal>
      )}
      {profileError && (
        <Alert color="red" data-testid="source-detail-profiler-error">
          {profileError}
        </Alert>
      )}
      {profileOutcomes && (
        <Alert
          color={profileOutcomes.every((o) => o.error == null) ? "green" : "orange"}
          data-testid="source-detail-profiler-outcomes"
        >
          {profileOutcomes.length === 0
            ? t("sourceDetailPanel.profilerNoMembers")
            : profileOutcomes.map((o) => (
                <Text key={o.table} size="sm">
                  {o.error == null
                    ? t("sourceDetailPanel.profilerRan", { table: o.table })
                    : t("sourceDetailPanel.profilerFailed", { table: o.table, error: o.error })}
                </Text>
              ))}
        </Alert>
      )}
    </Stack>
  );
}
