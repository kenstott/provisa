// Copyright (c) 2026 Kenneth Stott
// Canary: 3a8c5e17-f6d2-40b9-9c74-e25d81b0a6f4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: Apply, what it will do shown first. Applying is all or nothing: when the server refuses
// it, every problem that stops it is listed here, each naming what to do, and nothing is changed.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, List, Modal, Stack, Text } from "@mantine/core";
import { Upload } from "lucide-react";
import {
  ApplyRefused,
  applyOntologyImport,
  applyPreview,
  type ApplyResult,
  type ImportReview,
} from "../../../api/ontologyImport";
import { FormFooter } from "../../form/FormLayout";

export function ApplyImportDialog({
  review,
  onClose,
  onApplied,
}: {
  review: ImportReview;
  onClose: () => void;
  onApplied: (result: ApplyResult) => void;
}) {
  const { t } = useTranslation();
  const preview = applyPreview(review.items);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [problems, setProblems] = useState<string[]>([]);

  const run = async () => {
    setBusy(true);
    setError("");
    setProblems([]);
    try {
      onApplied(await applyOntologyImport(review.import.id));
    } catch (e) {
      if (e instanceof ApplyRefused) setProblems(e.problems.length ? e.problems : [e.message]);
      else setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const lines: [string, number][] = [
    ["previewCreate", preview.create],
    ["previewSameAs", preview.sameAs],
    ["previewEdges", preview.edges],
    ["previewWaiting", preview.waiting],
    ["previewExcluded", preview.excluded],
    ["previewUndecided", preview.undecided],
  ];

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("ontologyImport.applyTitle", { title: review.import.title })}
      size="lg"
      data-testid="ontology-apply-dialog"
    >
      <Stack gap="sm">
        <Text size="sm">{t("ontologyImport.applyIntro")}</Text>
        <Stack gap={4} data-testid="ontology-apply-preview">
          {lines.map(([key, count]) => (
            <Group key={key} gap="xs" data-testid={`ontology-apply-${key}`} data-count={count}>
              <Text size="sm" fw={600} w={40} ta="end">
                {count}
              </Text>
              <Text size="sm">{t(`ontologyImport.${key}`, { count })}</Text>
            </Group>
          ))}
        </Stack>
        {problems.length > 0 && (
          <Alert
            color="red"
            title={t("ontologyImport.applyRefused", { count: problems.length })}
            data-testid="ontology-apply-problems"
          >
            <List size="sm" spacing={4}>
              {problems.map((p) => (
                <List.Item key={p}>{p}</List.Item>
              ))}
            </List>
          </Alert>
        )}
        {error && (
          <Alert color="red" data-testid="ontology-apply-error">
            {error}
          </Alert>
        )}
        <FormFooter
          onCancel={onClose}
          action={
            <Button
              leftSection={<Upload size={14} />}
              onClick={() => void run()}
              loading={busy}
              data-testid="ontology-apply-confirm"
            >
              {t("ontologyImport.apply")}
            </Button>
          }
        />
      </Stack>
    </Modal>
  );
}
