// Copyright (c) 2026 Kenneth Stott
// Canary: c988b01f-f907-4492-bef8-3c7882e962af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: a modeling wizard's step in the + Model panel: the step strip, which shows the progress
// made, the step's own question in its method's words, and (amended 2026-11-27, after the source
// setup flow of REQ-2234) the step's choices as the panel's command bar: the step's primary
// action, Skip step where no later step reads the step's answers, Back after the first step, and
// Close.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Button, Input } from "@mantine/core";
import { FormField, FormFooter, FormLayout } from "../../../components/form/FormLayout";
import { PanelBody, PanelSection, PanelStrip } from "../../../components/list/PanelBody";
import { useLeaveGuard } from "../../../components/list/panelLeave";
import { useScreenInputs } from "../../../mcp/screenState";
import { StepStrip, type Step } from "../../../components/StepStrip";
import { FieldLabel } from "../FieldLabel";

/**
 * One step of a wizard in its panel: the step strip, then the step's title and what it asks, then
 * its fields, at the panel body's one rhythm (REQ-2277 rule 7, PanelBody).
 */
export function WizardStep({
  index,
  progress,
  steps,
  onStep,
  title,
  intro,
  testId,
  children,
}: {
  /** The step on screen. */
  index: number;
  /** The furthest step reached: the strip shows progress, not the step on screen. */
  progress: number;
  steps: readonly Step[];
  /** A step in the strip, up to the progress, was pressed: it opens. */
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
          active={progress}
          onStepClick={onStep}
          currentSelectable
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

/**
 * The step's choices, the panel's command bar (its pinned head, as in the source setup flow): the
 * primary action (Next, Review on the last question, Create on the review), Skip step where the
 * step offers it, Back from the second step on, and Close, which leaves through the panel's
 * unsaved-changes guard.
 */
export function WizardFooter({
  primaryLabel,
  onPrimary,
  primaryTestId = "wizard-next",
  creates = false,
  busy = false,
  onSkip,
  onBack,
  onClose,
  problem,
}: {
  primaryLabel: string;
  onPrimary: () => void;
  primaryTestId?: string;
  /** The primary action creates the model (the review's Create); every other step's only moves on. */
  creates?: boolean;
  busy?: boolean;
  /** Offered only on a step no later step reads: drops the step's answers and moves on. */
  onSkip?: () => void;
  /** Left out on the first step. */
  onBack?: () => void;
  onClose: () => void;
  problem: string | null;
}) {
  const { t } = useTranslation();
  const guard = useLeaveGuard();
  return (
    <FormFooter
      panel
      problem={problem}
      action={
        <>
          {/* REQ-2115: Polly asks the user before she presses Create, and nothing else here. */}
          <Button
            onClick={onPrimary}
            loading={busy}
            data-changes={creates}
            data-testid={primaryTestId}
          >
            {primaryLabel}
          </Button>
          {onSkip && (
            <Button onClick={onSkip} disabled={busy} data-changes={false} data-testid="wizard-skip">
              {t("modelingWizard.skipStep")}
            </Button>
          )}
          {onBack && (
            <Button onClick={onBack} disabled={busy} data-changes={false} data-testid="wizard-back">
              {t("modelingWizard.back")}
            </Button>
          )}
          <Button
            className="panel-head-end"
            onClick={() => guard(onClose)}
            disabled={busy}
            data-changes={false}
            data-testid="wizard-close"
          >
            {t("modelingWizard.close")}
          </Button>
        </>
      }
    />
  );
}
