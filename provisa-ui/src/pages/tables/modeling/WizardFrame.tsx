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
import { Button, Input } from "@mantine/core";
import { FormField, FormFooter, FormLayout } from "../../../components/form/FormLayout";
import { PanelBody, PanelSection, PanelStrip } from "../../../components/list/PanelBody";
import { useScreenInputs } from "../../../mcp/screenState";
import { StepStrip, type Step } from "../../../components/StepStrip";
import { FieldLabel } from "../FieldLabel";

/**
 * One step of a wizard in its panel: the step strip, then the step's title and what it asks, then
 * its fields, at the panel body's one rhythm (REQ-2277 rule 7, PanelBody).
 */
export function WizardStep({
  index,
  steps,
  onStep,
  title,
  intro,
  testId,
  children,
}: {
  index: number;
  steps: readonly Step[];
  /** A completed step in the strip was pressed. */
  onStep: (index: number) => void;
  title: string;
  intro: string;
  testId: string;
  /** The step's fields and its WizardFooter: laid out by the shared form layout. */
  children: ReactNode;
}) {
  const { t } = useTranslation();
  // REQ-2115: Polly's read_screen reads the step on screen; a wizard opens only on Tables.
  useScreenInputs("/tables", { wizardStep: { number: index + 1, count: steps.length, title } });
  return (
    <PanelBody data-testid={testId}>
      <PanelStrip>
        <StepStrip
          steps={steps}
          active={index}
          onStepClick={onStep}
          label={t("modelingWizard.stepsLabel")}
          testId="wizard-steps"
        />
      </PanelStrip>
      <PanelSection title={title} description={intro}>
        <FormLayout>{children}</FormLayout>
      </PanelSection>
    </PanelBody>
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
  creates = false,
}: {
  first: boolean;
  nextLabel: string;
  onNext: () => void;
  onBack: () => void;
  onCancel: () => void;
  problem: string | null;
  busy?: boolean;
  nextTestId?: string;
  /** Next creates the model (the review's Create); every other step's Next only moves on. */
  creates?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <FormFooter
      panel
      onCancel={onCancel}
      problem={problem}
      action={
        <>
          {/* REQ-2115: Polly asks the user before she presses Create, not Next or Back. */}
          <Button onClick={onNext} loading={busy} data-changes={creates} data-testid={nextTestId}>
            {nextLabel}
          </Button>
          {!first && (
            <Button onClick={onBack} disabled={busy} data-changes={false} data-testid="wizard-back">
              {t("modelingWizard.back")}
            </Button>
          )}
        </>
      }
    />
  );
}
