// Copyright (c) 2026 Kenneth Stott
// Canary: c37e5b90-8a14-4d2f-96e1-0b5d8f2a7c43
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189: the ERD bar's Layout menu. It names the layout shown, marks it unsaved once it is
// edited, and offers every saved layout, Automatic (the generated one: always there, never
// deleted), Save, Save as and Delete.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Button, Group, Menu, Modal, TextInput } from "@mantine/core";
import { Check, ChevronDown } from "lucide-react";
import type { SavedLayoutName } from "../../api/erdLayouts";
import { ConfirmDialog } from "../ConfirmDialog";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";

/** Names a layout to save the arrangement as. */
function SaveAsDialog({
  taken,
  onSave,
  onClose,
}: {
  taken: string[];
  onSave: (name: string) => Promise<boolean>;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const trimmed = name.trim();
  const clash = taken.includes(trimmed);
  return (
    <Modal opened onClose={onClose} title={t("erdLayouts.saveAsTitle")} centered>
      <FormLayout>
        <FormField size="medium">
          <TextInput
            label={t("erdLayouts.name")}
            value={name}
            onChange={(e) => setName(e.currentTarget.value)}
            error={clash ? t("erdLayouts.nameTaken", { name: trimmed }) : undefined}
            required
            data-autofocus
            data-testid="erd-layout-name"
          />
        </FormField>
        <FormFooter
          onSave={() => onSave(trimmed)}
          onCancel={onClose}
          saveProps={{ disabled: trimmed === "" || clash, "data-testid": "erd-layout-save-as-ok" }}
        />
      </FormLayout>
    </Modal>
  );
}

export function ErdLayoutMenu({
  layouts,
  current,
  unsaved,
  onChoose,
  onSave,
  onDelete,
}: {
  layouts: SavedLayoutName[];
  /** The layout shown; null for Automatic. */
  current: string | null;
  unsaved: boolean;
  onChoose: (name: string | null) => void;
  /** Save the arrangement as `name`: the current layout's own, or a new one. False when it was
   * refused; the bar says why. */
  onSave: (name: string) => Promise<boolean>;
  onDelete: (name: string) => Promise<boolean>;
}) {
  const { t } = useTranslation();
  const [savingAs, setSavingAs] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const automatic = t("erdLayouts.automatic");
  const choice = (name: string | null, label: string, testId: string) => (
    <Menu.Item
      key={testId}
      onClick={() => onChoose(name)}
      leftSection={current === name ? <Check size={12} /> : <span style={{ width: 12 }} />}
      data-testid={testId}
    >
      {label}
    </Menu.Item>
  );
  return (
    <Group gap={6} wrap="nowrap">
      <Menu position="bottom-start" transitionProps={{ duration: 0 }}>
        <Menu.Target>
          <Tip text={t("erdLayouts.menuHint")}>
            <Button
              size="xs"
              h={30}
              variant="default"
              rightSection={<ChevronDown size={12} />}
              data-testid="erd-layout-menu"
            >
              {t("erdLayouts.layout", { name: current ?? automatic })}
            </Button>
          </Tip>
        </Menu.Target>
        <Menu.Dropdown data-testid="erd-layout-dropdown">
          <Menu.Label>{t("erdLayouts.layouts")}</Menu.Label>
          {choice(null, automatic, "erd-layout-automatic")}
          {layouts.map((l) => choice(l.name, l.name, `erd-layout-choose-${l.name}`))}
          <Menu.Divider />
          <Menu.Item
            disabled={current === null || !unsaved}
            onClick={() => current !== null && void onSave(current)}
            data-testid="erd-layout-save"
          >
            {t("erdLayouts.save")}
          </Menu.Item>
          <Menu.Item onClick={() => setSavingAs(true)} data-testid="erd-layout-save-as">
            {t("erdLayouts.saveAs")}
          </Menu.Item>
          <Menu.Item
            color="red"
            disabled={current === null}
            onClick={() => setDeleting(true)}
            data-testid="erd-layout-delete"
          >
            {t("erdLayouts.delete")}
          </Menu.Item>
        </Menu.Dropdown>
      </Menu>
      {unsaved && (
        <Tip text={t("erdLayouts.unsavedHint")}>
          <Badge size="sm" color="orange" variant="light" data-testid="erd-layout-unsaved">
            {t("erdLayouts.unsaved")}
          </Badge>
        </Tip>
      )}
      {savingAs && (
        <SaveAsDialog
          taken={layouts.map((l) => l.name)}
          onClose={() => setSavingAs(false)}
          onSave={async (name) => {
            if (await onSave(name)) setSavingAs(false);
            return true;
          }}
        />
      )}
      {current !== null && (
        <ConfirmDialog
          opened={deleting}
          onCancel={() => setDeleting(false)}
          title={t("erdLayouts.deleteTitle", { name: current })}
          consequence={t("erdLayouts.deleteConsequence")}
          confirmLabel={t("erdLayouts.delete")}
          onConfirm={async () => {
            await onDelete(current);
            setDeleting(false);
          }}
          testId="erd-layout-delete-confirm"
        />
      )}
    </Group>
  );
}
