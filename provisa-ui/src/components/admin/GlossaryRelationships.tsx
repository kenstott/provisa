// Copyright (c) 2026 Kenneth Stott
// Canary: d8ab0549-f116-4818-a5ed-f80c3f153554
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Split out of GlossaryTab.tsx to keep that file under the max-lines limit. Owns only the
// Relationships table + add-edge row; the edit-mode toggle it renders under is state the parent
// still holds, since the same toggle also gates whether the heading shows the pencil.

import { ActionIcon, Anchor, Button, Group, Select, Table, Text } from "@mantine/core";
import type { TFunction } from "i18next";
import { ArrowLeftRight, Trash2 } from "lucide-react";
import type { GlossaryRelType, GlossaryTermDetail } from "../../api/glossary";

interface RelTypeOption {
  value: string;
  label: string;
}

interface GlossaryRelationshipsProps {
  detail: GlossaryTermDetail;
  canEdit: boolean;
  relEditMode: boolean;
  relTypeOptions: RelTypeOption[];
  otherTermOptions: { value: string; label: string }[];
  edgeTermId: string | null;
  setEdgeTermId: (v: string | null) => void;
  edgeRelType: string | null;
  setEdgeRelType: (v: string | null) => void;
  edgeFlipped: boolean;
  setEdgeFlipped: (fn: (flipped: boolean) => boolean) => void;
  navigateToTerm: (termId: number) => void;
  act: (fn: () => Promise<void>, successMessage?: string) => Promise<void>;
  relLabel: (options: RelTypeOption[], relType: string) => string;
  t: TFunction;
  retypeGlossaryEdge: (
    fromId: number,
    toId: number,
    relType: GlossaryRelType,
    next: GlossaryRelType,
  ) => Promise<void>;
  removeGlossaryEdge: (fromId: number, toId: number, relType: GlossaryRelType) => Promise<void>;
  addGlossaryEdge: (fromId: number, toId: number, relType: GlossaryRelType) => Promise<void>;
}

export function GlossaryRelationships({
  detail,
  canEdit,
  relEditMode,
  relTypeOptions,
  otherTermOptions,
  edgeTermId,
  setEdgeTermId,
  edgeRelType,
  setEdgeRelType,
  edgeFlipped,
  setEdgeFlipped,
  navigateToTerm,
  act,
  relLabel,
  t,
  retypeGlossaryEdge,
  removeGlossaryEdge,
  addGlossaryEdge,
}: GlossaryRelationshipsProps) {
  const currentTermNode = (
    <Text size="sm" fw={500} data-testid="glossary-edge-add-current-term">
      {detail.name}
    </Text>
  );
  const pickedTermNode = (
    <Select
      label={t("glossaryTab.edgeTermLabel")}
      data={otherTermOptions}
      value={edgeTermId}
      onChange={setEdgeTermId}
      searchable
      w={220}
      data-testid="glossary-edge-term-select"
    />
  );

  return (
    <>
      <Table
        withTableBorder={false}
        withRowBorders={false}
        verticalSpacing={4}
        horizontalSpacing="xs"
        style={{ width: "fit-content" }}
        data-testid="glossary-relationships-table"
      >
        <Table.Tbody>
          {detail.edges_out.map((edge) => (
            <Table.Tr
              key={`out:${edge.term_id}:${edge.rel_type}`}
              data-testid={`glossary-edge-out-${edge.term_id}-${edge.rel_type}`}
            >
              {/* Read as {current term} {relationship} {connected term} (REQ-1590). First-column
                  name is fixed width and non-wrapping so the relationship and target columns stay
                  aligned across rows. */}
              <Table.Td style={{ width: 180, whiteSpace: "nowrap" }}>
                <Text size="sm">{detail.name}</Text>
              </Table.Td>
              {/* The type is part of the edge's identity, so correcting it is a retype, not a
                  delete plus an add — the curator is fixing one statement about one pair of
                  terms. */}
              <Table.Td>
                {canEdit && relEditMode ? (
                  <Select
                    data={relTypeOptions}
                    value={edge.rel_type}
                    onChange={(next) =>
                      next &&
                      next !== edge.rel_type &&
                      void act(() =>
                        retypeGlossaryEdge(
                          detail.id,
                          edge.term_id,
                          edge.rel_type,
                          next as GlossaryRelType,
                        ),
                      )
                    }
                    size="xs"
                    w={190}
                    aria-label={t("glossaryTab.edgeRelLabel")}
                    data-testid={`glossary-edge-out-rel-${edge.term_id}`}
                  />
                ) : (
                  <Text size="sm" w={190}>
                    {relLabel(relTypeOptions, edge.rel_type)}
                  </Text>
                )}
              </Table.Td>
              <Table.Td>
                <Anchor
                  component="a"
                  href={`?term=${edge.term_id}`}
                  size="sm"
                  onClick={(e) => {
                    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
                    e.preventDefault();
                    navigateToTerm(edge.term_id);
                  }}
                  data-testid={`glossary-edge-out-link-${edge.term_id}`}
                >
                  {edge.name}
                </Anchor>
              </Table.Td>
              <Table.Td>
                {canEdit && relEditMode && (
                  <ActionIcon
                    variant="subtle"
                    color="red"
                    size="sm"
                    aria-label={t("glossaryTab.removeEdge")}
                    onClick={() =>
                      void act(() => removeGlossaryEdge(detail.id, edge.term_id, edge.rel_type))
                    }
                  >
                    <Trash2 size={13} />
                  </ActionIcon>
                )}
              </Table.Td>
            </Table.Tr>
          ))}
          {detail.edges_in.map((edge) => (
            <Table.Tr
              key={`in:${edge.term_id}:${edge.rel_type}`}
              data-testid={`glossary-edge-in-${edge.term_id}-${edge.rel_type}`}
            >
              {/* Read as {connected term} {relationship} {current term} (REQ-1590) — the stored
                  type is forward regardless of which term's page this is, so the picker and label
                  are the same relTypeOptions used for edges_out. */}
              <Table.Td style={{ width: 180, whiteSpace: "nowrap" }}>
                <Anchor
                  component="a"
                  href={`?term=${edge.term_id}`}
                  size="sm"
                  onClick={(e) => {
                    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
                    e.preventDefault();
                    navigateToTerm(edge.term_id);
                  }}
                  data-testid={`glossary-edge-in-link-${edge.term_id}`}
                >
                  {edge.name}
                </Anchor>
              </Table.Td>
              <Table.Td>
                {canEdit && relEditMode ? (
                  <Select
                    data={relTypeOptions}
                    value={edge.rel_type}
                    onChange={(next) =>
                      next &&
                      next !== edge.rel_type &&
                      void act(() =>
                        retypeGlossaryEdge(
                          edge.term_id,
                          detail.id,
                          edge.rel_type,
                          next as GlossaryRelType,
                        ),
                      )
                    }
                    size="xs"
                    w={190}
                    aria-label={t("glossaryTab.edgeRelLabel")}
                    data-testid={`glossary-edge-in-rel-${edge.term_id}`}
                  />
                ) : (
                  <Text size="sm" w={190}>
                    {relLabel(relTypeOptions, edge.rel_type)}
                  </Text>
                )}
              </Table.Td>
              <Table.Td>
                <Text size="sm">{detail.name}</Text>
              </Table.Td>
              <Table.Td>
                {canEdit && relEditMode && (
                  <ActionIcon
                    variant="subtle"
                    color="red"
                    size="sm"
                    aria-label={t("glossaryTab.removeEdge")}
                    onClick={() =>
                      void act(() => removeGlossaryEdge(edge.term_id, detail.id, edge.rel_type))
                    }
                  >
                    <Trash2 size={13} />
                  </ActionIcon>
                )}
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      {detail.edges_out.length === 0 && detail.edges_in.length === 0 && (
        <Text size="xs" c="dimmed">
          {t("glossaryTab.noEdges")}
        </Text>
      )}
      {canEdit && relEditMode && (
        <Group align="flex-end" gap="sm">
          {edgeFlipped ? pickedTermNode : currentTermNode}
          <Select
            label={t("glossaryTab.edgeRelLabel")}
            data={relTypeOptions}
            value={edgeRelType}
            onChange={setEdgeRelType}
            w={180}
            data-testid="glossary-edge-rel-select"
          />
          {edgeFlipped ? currentTermNode : pickedTermNode}
          <Button
            variant="default"
            disabled={edgeTermId === null || edgeRelType === null}
            onClick={() =>
              void act(() =>
                addGlossaryEdge(
                  edgeFlipped ? Number(edgeTermId) : detail.id,
                  edgeFlipped ? detail.id : Number(edgeTermId),
                  edgeRelType as GlossaryRelType,
                ),
              )
            }
            data-testid="glossary-edge-add-btn"
          >
            {t("glossaryTab.addEdge")}
          </Button>
          <ActionIcon
            variant="default"
            aria-label={t("glossaryTab.flipEdgeLabel")}
            onClick={() => setEdgeFlipped((flipped) => !flipped)}
            data-testid="glossary-edge-flip-btn"
          >
            <ArrowLeftRight size={14} />
          </ActionIcon>
        </Group>
      )}
    </>
  );
}
