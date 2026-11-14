// Copyright (c) 2026 Kenneth Stott
// Canary: 3e9a7c51-0b2d-4f86-a1c4-7d5e2b9f6a03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: discovery asks before it runs. The kinds to look for, both ticked; Semantics greyed
// out and unticked, with the reason, when the model assigned to relationship inference has no key.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Modal, Text } from "@mantine/core";
import { fetchDiscoveryKinds, type DiscoveryKind, type DiscoveryKinds } from "../../api/admin";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

interface DiscoverDialogProps {
  opened: boolean;
  onCancel: () => void;
  /** Run the kinds ticked. The dialog closes; the page shows the run and its result. */
  onRun: (kinds: DiscoveryKind[]) => void;
}

export function DiscoverDialog({ opened, onCancel, onRun }: DiscoverDialogProps) {
  const { t } = useTranslation();
  // The choices mount each time the dialog opens: both ticked again, and which kinds can run read
  // again, so a key set meanwhile counts.
  return (
    <Modal
      opened={opened}
      onClose={onCancel}
      title={t("discoverDialog.title")}
      centered
      data-testid="discover-dialog"
    >
      {opened && <DiscoverChoices onCancel={onCancel} onRun={onRun} />}
    </Modal>
  );
}

function DiscoverChoices({ onCancel, onRun }: Omit<DiscoverDialogProps, "opened">) {
  const { t } = useTranslation();
  const [kinds, setKinds] = useState<DiscoveryKinds | null>(null);
  const [readError, setReadError] = useState("");
  const [foreignKeys, setForeignKeys] = useState(true);
  const [semantics, setSemantics] = useState(true);

  useEffect(() => {
    let live = true;
    fetchDiscoveryKinds()
      .then((k) => {
        if (!live) return;
        setKinds(k);
        if (!k.semantics.available) setSemantics(false);
      })
      .catch((e) => {
        if (live) setReadError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      live = false;
    };
  }, []);

  const semanticsAvailable = kinds?.semantics.available === true;
  const chosen: DiscoveryKind[] = [
    ...(foreignKeys ? (["foreign_keys"] as const) : []),
    ...(semantics && semanticsAvailable ? (["semantics"] as const) : []),
  ];

  return (
    <>
      <Text mb="md">{t("discoverDialog.intro")}</Text>
      {readError && (
        <Alert color="red" mb="md" data-testid="discover-dialog-error">
          {readError}
        </Alert>
      )}
      <FormLayout>
        <FormChecks column>
          <FormField size="long">
            <Checkbox
              label={t("discoverDialog.foreignKeys")}
              checked={foreignKeys}
              onChange={(e) => setForeignKeys(e.currentTarget.checked)}
              data-testid="discover-kind-foreign-keys"
            />
          </FormField>
          <FormField size="long">
            <Checkbox
              label={
                kinds !== null && !semanticsAvailable
                  ? t("discoverDialog.semanticsNoKey", { model: kinds.semantics.model })
                  : t("discoverDialog.semantics")
              }
              checked={semantics && semanticsAvailable}
              disabled={!semanticsAvailable}
              onChange={(e) => setSemantics(e.currentTarget.checked)}
              data-testid="discover-kind-semantics"
            />
          </FormField>
        </FormChecks>
      </FormLayout>
      <FormFooter
        onCancel={onCancel}
        cancelProps={{ "data-testid": "discover-cancel" }}
        action={
          <Button
            onClick={() => onRun(chosen)}
            disabled={kinds === null || chosen.length === 0}
            data-testid="discover-run"
          >
            {t("discoverDialog.run")}
          </Button>
        }
      />
    </>
  );
}
