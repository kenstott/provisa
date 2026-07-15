// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Box,
  Button,
  Group,
  Modal,
  Select,
  Stack,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import type { Extension } from "@codemirror/state";
import type { ViewColumnConfig } from "./types";
import type { Domain } from "../../types/admin";

interface ViewModalProps {
  viewModal: boolean;
  setViewModal: React.Dispatch<React.SetStateAction<boolean>>;
  viewMsg: string;
  canCreateView: boolean;
  handleSaveView: () => void;
  viewSaving: boolean;
  viewId: string;
  setViewId: React.Dispatch<React.SetStateAction<string>>;
  viewDomainId: string;
  setViewDomainId: React.Dispatch<React.SetStateAction<string>>;
  viewHasParams: boolean;
  viewDescription: string;
  setViewDescription: React.Dispatch<React.SetStateAction<string>>;
  viewSqlNormalized: string;
  viewSqlExtensions: Extension[];
  domainMap: Record<string, Domain>;
  savedViewId: number | null;
  setSavedViewId: React.Dispatch<React.SetStateAction<number | null>>;
  setViewColumns: React.Dispatch<React.SetStateAction<ViewColumnConfig[]>>;
  onNavigateToViews: () => void;
  onCloseConfirmation: () => void;
}

export function ViewModal({
  viewModal,
  setViewModal,
  viewMsg,
  canCreateView,
  handleSaveView,
  viewSaving,
  viewId,
  setViewId,
  viewDomainId,
  setViewDomainId,
  viewHasParams,
  viewDescription,
  setViewDescription,
  viewSqlNormalized,
  viewSqlExtensions,
  domainMap,
  savedViewId,
  setSavedViewId,
  setViewColumns,
  onNavigateToViews,
  onCloseConfirmation,
}: ViewModalProps) {
  const { t } = useTranslation();

  const closeConfirmation = () => {
    setSavedViewId(null);
    setViewModal(false);
    setViewColumns([]);
    onCloseConfirmation();
  };

  const domainOptions = Object.values(domainMap)
    .filter((d) => d.id && d.id !== "meta" && d.id !== "ops")
    .map((d) => ({
      value: d.id,
      label: d.description ? `${d.id} — ${d.description}` : d.id,
    }));

  return (
    <>
      <Modal
        opened={viewModal}
        onClose={() => setViewModal(false)}
        title={
          <Title order={4}>
            {canCreateView ? t("sqlViewModal.titleCreate") : t("sqlViewModal.titleRequest")}
          </Title>
        }
        size="90vw"
        styles={{ body: { display: "flex", flexDirection: "column", maxHeight: "70vh" } }}
        data-testid="view-modal"
      >
        <Group justify="flex-end" mb="sm" style={{ flexShrink: 0 }}>
          {viewMsg && (
            <Text size="xs" c={viewMsg.startsWith("Error") ? "var(--destructive)" : "var(--approve)"}>
              {viewMsg}
            </Text>
          )}
          <Button
            onClick={handleSaveView}
            disabled={viewSaving || !viewId.trim() || !viewDomainId.trim() || viewHasParams}
            loading={viewSaving}
            size="xs"
            data-testid="save-view-button"
          >
            {viewSaving
              ? t("sqlViewModal.saving")
              : canCreateView
                ? t("sqlViewModal.create")
                : t("sqlViewModal.submitRequest")}
          </Button>
        </Group>
        {!canCreateView && (
          <Alert color="yellow" mb="sm" style={{ flexShrink: 0 }}>
            {t("sqlViewModal.noPermissionNotice")}
          </Alert>
        )}
        <Stack gap="sm" style={{ overflow: "auto", flex: 1, paddingRight: "1rem" }}>
          <Group gap="sm" style={{ flexShrink: 0 }} grow>
            <TextInput
              label={t("sqlViewModal.alias")}
              required
              value={viewId}
              onChange={(e) => setViewId(e.target.value)}
              placeholder={t("sqlViewModal.aliasPlaceholder")}
              data-testid="view-alias-input"
            />
            <Select
              label={t("sqlViewModal.domain")}
              required
              value={viewDomainId || null}
              onChange={(v) => setViewDomainId(v ?? "")}
              placeholder={t("sqlViewModal.domainSelectPlaceholder")}
              data={domainOptions}
              data-testid="view-domain-select"
            />
          </Group>
          <Textarea
            label={t("sqlViewModal.description")}
            value={viewDescription}
            onChange={(e) => setViewDescription(e.target.value)}
            placeholder={t("sqlViewModal.descriptionPlaceholder")}
            rows={2}
            resize="vertical"
            style={{ flexShrink: 0 }}
            data-testid="view-description-input"
          />
          {viewHasParams && (
            <Alert color="red" style={{ flexShrink: 0 }}>
              {t("sqlViewModal.hasParamsWarning")}
            </Alert>
          )}
          <Box
            style={{
              resize: "vertical",
              overflow: "auto",
              minHeight: 80,
              height: 120,
              flexShrink: 0,
            }}
          >
            <CodeMirror
              value={viewSqlNormalized}
              extensions={viewSqlExtensions}
              theme={oneDark}
              editable={false}
              height="100%"
              basicSetup={{ lineNumbers: false, foldGutter: false }}
            />
          </Box>
        </Stack>
      </Modal>
      <Modal
        opened={savedViewId !== null}
        onClose={closeConfirmation}
        title={<Title order={4}>{t("sqlViewModal.savedTitle")}</Title>}
        centered
        data-testid="view-saved-modal"
      >
        <Text mb="lg" c="var(--text-muted)">
          {canCreateView
            ? t("sqlViewModal.savedMessageCreated", { viewId })
            : t("sqlViewModal.savedMessageSubmitted", { viewId })}
        </Text>
        <Group justify="flex-end" gap="sm">
          <Button variant="default" onClick={closeConfirmation} data-testid="view-saved-close-button">
            {t("sqlViewModal.close")}
          </Button>
          <Button
            onClick={() => {
              closeConfirmation();
              onNavigateToViews();
            }}
            data-testid="view-saved-navigate-button"
          >
            {t("sqlViewModal.editInModelViews")}
          </Button>
        </Group>
      </Modal>
    </>
  );
}
