// Copyright (c) 2026 Kenneth Stott
// Canary: 1e7a4c93-d2b8-4f05-a6c1-5f39e08b72d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: one proposed alignment of an imported item, as the steward decides it: why it was
// proposed (its basis), what it says read as a sentence in its direction, the term or other item
// it aligns with (a term's role, status and grounding beside its name), and Approve or Reject. A
// term aligned as the same entity may take the item's definition in place of its own.

import { useTranslation } from "react-i18next";
import { Badge, Button, Checkbox, Group, Stack, Text } from "@mantine/core";
import { Check, X } from "lucide-react";
import type { Alignment, ImportItem } from "../../../api/ontologyImport";
import { Tip } from "../../Tip";
import { TermGroundingChip, TermRoleBadge, TermStatusBadge } from "../TermRoleBadge";
import { basisKey, directionPhrase } from "./alignmentPhrase";

const STATUS_COLOUR: Record<Alignment["status"], string> = {
  proposed: "yellow",
  approved: "teal",
  rejected: "gray",
  written: "blue",
};

function Target({ a }: { a: Alignment }) {
  const { t } = useTranslation();
  if (a.term && "outside_domains" in a.term)
    return (
      <Text size="sm" c="dimmed" data-testid={`ontology-alignment-target-${a.id}`}>
        {t("ontologyImport.outsideDomains")}
      </Text>
    );
  if (a.term)
    return (
      <Group gap={6} wrap="wrap" data-testid={`ontology-alignment-target-${a.id}`}>
        <Text size="sm" fw={500}>
          {a.term.name}
        </Text>
        <TermRoleBadge role={a.term.role} />
        <TermStatusBadge status={a.term.status} />
        <TermGroundingChip grounding={a.term.grounding} />
      </Group>
    );
  return (
    <Group gap={6} data-testid={`ontology-alignment-target-${a.id}`}>
      <Text size="sm" fw={500}>
        {a.other_item?.name ?? "—"}
      </Text>
      <Badge size="xs" variant="outline" color="gray">
        {t("ontologyImport.inThisImport")}
      </Badge>
    </Group>
  );
}

export function AlignmentRow({
  item,
  alignment: a,
  canDecide,
  selected,
  onSelect,
  useDefinition,
  onUseDefinition,
  onApprove,
  onReject,
}: {
  item: ImportItem;
  alignment: Alignment;
  /** The item is still pending and the alignment not yet written. */
  canDecide: boolean;
  selected: boolean;
  onSelect: (on: boolean) => void;
  useDefinition: boolean;
  onUseDefinition: (on: boolean) => void;
  onApprove: () => void;
  onReject: () => void;
}) {
  const { t } = useTranslation();
  const sameAs = a.rel_type === "SAME_AS";
  const proposed = a.status === "proposed";
  return (
    <Group
      gap="sm"
      wrap="nowrap"
      align="flex-start"
      data-testid={`ontology-alignment-${a.id}`}
      data-status={a.status}
    >
      {canDecide && proposed ? (
        <Checkbox
          size="xs"
          mt={4}
          checked={selected}
          onChange={(e) => onSelect(e.currentTarget.checked)}
          aria-label={t("ontologyImport.selectAlignment", { phrase: directionPhrase(item, a, t) })}
          data-testid={`ontology-alignment-select-${a.id}`}
        />
      ) : null}
      <Stack gap={4} style={{ flex: 1, minWidth: 0 }}>
        <Group gap={6} wrap="wrap">
          <Tip text={t(`ontologyImport.basisHelp_${basisKey(a.basis)}`)}>
            <Badge
              size="xs"
              variant="light"
              color={a.basis === "steward" ? "violet" : "cyan"}
              data-testid={`ontology-alignment-basis-${a.id}`}
            >
              {t(`ontologyImport.basis_${basisKey(a.basis)}`)}
            </Badge>
          </Tip>
          <Badge size="xs" variant="dot" color={STATUS_COLOUR[a.status]}>
            {t(`ontologyImport.alignmentStatus_${a.status}`)}
          </Badge>
          <Text size="sm" data-testid={`ontology-alignment-phrase-${a.id}`}>
            {directionPhrase(item, a, t)}
          </Text>
        </Group>
        <Target a={a} />
        {sameAs && item.definition && (canDecide || a.use_definition) && (
          <Tip text={t("ontologyImport.useDefinitionHelp")}>
            <Checkbox
              size="xs"
              label={t("ontologyImport.useDefinition")}
              checked={proposed ? useDefinition : a.use_definition}
              disabled={!canDecide || !proposed}
              onChange={(e) => onUseDefinition(e.currentTarget.checked)}
              data-testid={`ontology-alignment-use-definition-${a.id}`}
            />
          </Tip>
        )}
      </Stack>
      {canDecide && (
        <Group gap={4} wrap="nowrap" data-toolbar>
          {a.status !== "approved" && (
            <Button
              size="compact-xs"
              variant="light"
              color="teal"
              leftSection={<Check size={12} />}
              onClick={onApprove}
              data-testid={`ontology-alignment-approve-${a.id}`}
            >
              {t("ontologyImport.approve")}
            </Button>
          )}
          {a.status !== "rejected" && (
            <Button
              size="compact-xs"
              variant="subtle"
              color="gray"
              leftSection={<X size={12} />}
              onClick={onReject}
              data-testid={`ontology-alignment-reject-${a.id}`}
            >
              {t("ontologyImport.reject")}
            </Button>
          )}
        </Group>
      )}
    </Group>
  );
}
