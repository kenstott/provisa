// Copyright (c) 2026 Kenneth Stott
// Canary: c988b01f-f907-4492-bef8-3c7882e962af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: a modeling wizard's step in the + Model panel: which step of how many, the step's own
// question in its method's words, and Next / Back / Cancel as the panel's actions.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Button, Input, Text } from "@mantine/core";
import { FormField, FormFooter } from "../../../components/form/FormLayout";
import { FieldLabel } from "../FieldLabel";

/** Which step this is, its title and what it asks, at the head of the step. */
export function StepHead({
  index,
  count,
  title,
  intro,
  testId,
}: {
  index: number;
  count: number;
  title: string;
  intro: string;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="full">
      <div data-testid={testId}>
        <Text size="xs" c="dimmed" data-testid="wizard-step-count">
          {t("modelingWizard.stepOf", { index: index + 1, count })}
        </Text>
        <Text fw={600}>{title}</Text>
        <Text size="sm" c="dimmed">
          {intro}
        </Text>
      </div>
    </FormField>
  );
}

/** A list's heading inside a step, in the label slot so it reads as the form's other labels. */
export function ListHead({ text, help }: { text: string; help: string }) {
  return (
    <FormField size="full">
      <Input.Label component="div">
        <FieldLabel text={text} help={help} />
      </Input.Label>
    </FormField>
  );
}

/** A list's Add button, on a line of its own after the list. */
export function AddButton({
  onClick,
  testId,
  children,
}: {
  onClick: () => void;
  testId: string;
  children: ReactNode;
}) {
  return (
    <FormField size="full">
      <Button variant="light" size="xs" w="fit-content" onClick={onClick} data-testid={testId}>
        {children}
      </Button>
    </FormField>
  );
}

/** The step's actions: Next (Review on the last question), Back after the first, and Cancel. */
export function WizardFooter({
  first,
  nextLabel,
  onNext,
  onBack,
  onCancel,
  problem,
  busy = false,
  nextTestId = "wizard-next",
}: {
  first: boolean;
  nextLabel: string;
  onNext: () => void;
  onBack: () => void;
  onCancel: () => void;
  problem: string | null;
  busy?: boolean;
  nextTestId?: string;
}) {
  const { t } = useTranslation();
  return (
    <FormFooter
      panel
      onCancel={onCancel}
      problem={problem}
      action={
        <>
          <Button onClick={onNext} loading={busy} data-testid={nextTestId}>
            {nextLabel}
          </Button>
          {!first && (
            <Button onClick={onBack} disabled={busy} data-testid="wizard-back">
              {t("modelingWizard.back")}
            </Button>
          )}
        </>
      }
    />
  );
}
