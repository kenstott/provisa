// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2e8b71-0c94-4f3a-a6e8-91b7c4d03f26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387 (RELATED_TO IS THE LAST RESORT; RELATED_TO HOLDS THE PLACE; THE SHAPES THAT RESOLVE A
// RELATED PAIR): everywhere a relationship type is picked or proposed steers to the types that
// ground. The picker shows each type's test, a term held only by RELATED_TO says what may resolve
// it, and the assistant's RELATED_TO proposals come back with the shapes that would resolve them.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Anchor,
  Button,
  Checkbox,
  Group,
  Modal,
  Stack,
  Text,
  VisuallyHidden,
} from "@mantine/core";
import { Tip } from "../Tip";
import { BookOpen } from "lucide-react";
import {
  GROUNDING_GUIDE_URL,
  GROUNDING_REL_TYPES,
  type RelatedToReview,
  type ResolveShape,
} from "../../api/glossary";

/** The types whose test the picker shows: the three that ground, and the placeholder. */
const TESTED: readonly string[] = [...GROUNDING_REL_TYPES, "RELATED_TO"];

/** A picker option: its label, and under it the type's one-line test when it has one. */
export function RelTypeOptionView({ value, label }: { value: string; label: string }) {
  const { t } = useTranslation();
  return (
    <Stack gap={0} data-testid={`glossary-rel-option-${value}`}>
      <Text size="sm">{label}</Text>
      {TESTED.includes(value) && (
        <Text size="xs" c="dimmed" style={{ whiteSpace: "normal" }}>
          {t(`glossaryTab.relTest_${value}`)}
        </Text>
      )}
    </Stack>
  );
}

/** The docs section on building a glossary that grounds, opened beside the app. */
export function GroundingGuideLink({ testId, children }: { testId: string; children?: ReactNode }) {
  const { t } = useTranslation();
  return (
    <Anchor
      href={GROUNDING_GUIDE_URL}
      target="_blank"
      rel="noreferrer"
      size="xs"
      data-testid={testId}
    >
      <Group gap={4} wrap="nowrap" component="span">
        <BookOpen size={12} aria-hidden />
        {children ?? t("glossaryTab.groundingGuide")}
      </Group>
    </Anchor>
  );
}

/** The term list's filter to the terms on a RELATED_TO edge, with the way to the guidance. */
export function RelatedToFilter({
  checked,
  onChange,
}: {
  checked: boolean;
  onChange: (on: boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <Group gap={4} wrap="nowrap">
      <Checkbox
        size="xs"
        label={t("glossaryTab.relatedToOnly")}
        checked={checked}
        onChange={(e) => onChange(e.currentTarget.checked)}
        styles={{ label: { whiteSpace: "nowrap" } }}
        data-testid="glossary-related-to-only"
      />
      <Tip text={t("glossaryTab.relatedToOnlyHint")}>
        <span>
          <GroundingGuideLink testId="glossary-related-to-guide">
            <VisuallyHidden>{t("glossaryTab.relatedToOnlyHint")}</VisuallyHidden>
          </GroundingGuideLink>
        </span>
      </Tip>
    </Group>
  );
}

/** A term held only by RELATED_TO and not grounded: what may resolve it. Guidance, never a block. */
export function RelatedToHint() {
  const { t } = useTranslation();
  return (
    <Alert color="blue" variant="light" data-testid="glossary-related-to-hint">
      <Stack gap={4}>
        <Text size="sm">{t("glossaryTab.relatedToHint")}</Text>
        <Text size="sm">{t("glossaryTab.relatedToShapes")}</Text>
        <GroundingGuideLink testId="glossary-related-to-hint-guide" />
      </Stack>
    </Alert>
  );
}

/** One shape for a pair, as the edges it would add, with the candidate names for the new term. */
function ShapeLine({
  shape,
  candidates,
  from,
  to,
}: {
  shape: ResolveShape;
  candidates: string[];
  from: string;
  to: string;
}) {
  const { t } = useTranslation();
  return (
    <Text size="sm" data-testid={`glossary-review-shape-${shape}`}>
      <Text span fw={600}>
        {t(`glossaryTab.shape_${shape}`)}
      </Text>
      {": "}
      {t(`glossaryTab.shapeEdges_${shape}`, {
        from,
        to,
        term: candidates.join(" / "),
        interpolation: { escapeValue: false },
      })}
    </Text>
  );
}

/** After Generate Relationships: each RELATED_TO the assistant added, and what may resolve it. */
export function RelatedToReviewModal({
  review,
  onClose,
  onShowTerm,
}: {
  review: RelatedToReview[];
  onClose: () => void;
  onShowTerm: (termId: number) => void;
}) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={review.length > 0}
      onClose={onClose}
      title={t("glossaryTab.reviewTitle")}
      size="lg"
      data-testid="glossary-related-to-review"
    >
      <Stack gap="md">
        <Text size="sm">{t("glossaryTab.reviewIntro")}</Text>
        {review.map((r) => (
          <Stack
            key={`${r.from_term_id}:${r.to_term_id}`}
            gap={4}
            data-testid={`glossary-review-pair-${r.from_term_id}-${r.to_term_id}`}
          >
            <Group gap={6} wrap="wrap">
              <Anchor
                component="button"
                size="sm"
                fw={600}
                onClick={() => onShowTerm(r.from_term_id)}
              >
                {r.from}
              </Anchor>
              <Text size="sm">{t("glossaryTab.rel_RELATED_TO")}</Text>
              <Anchor
                component="button"
                size="sm"
                fw={600}
                onClick={() => onShowTerm(r.to_term_id)}
              >
                {r.to}
              </Anchor>
            </Group>
            <Text size="xs" c="dimmed">
              {r.resolve.length
                ? t("glossaryTab.reviewMayWantResolving")
                : t("glossaryTab.reviewNoShapes")}
            </Text>
            {r.resolve.map((s) => (
              <ShapeLine
                key={s.shape}
                shape={s.shape}
                candidates={s.candidates}
                from={r.from}
                to={r.to}
              />
            ))}
          </Stack>
        ))}
        <Group justify="space-between">
          <GroundingGuideLink testId="glossary-review-guide" />
          <Button variant="default" onClick={onClose} data-testid="glossary-review-close">
            {t("glossaryTab.reviewClose")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
