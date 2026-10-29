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

import { useState } from "react";
import { ActionIcon, Anchor, Button, Group, Select, Table, Text } from "@mantine/core";
import type { TFunction } from "i18next";
import { ArrowLeftRight, Trash2 } from "lucide-react";
import {
  addGlossaryEdge as postEdge,
  shapeEdges,
  type GlossaryRelType,
  type GlossaryTermDetail,
  type ResolveShape,
} from "../../api/glossary";
import { RelatedToDialog, ShapeTermForm, type PairTerm } from "./RelatedToDialog";
import { GroundingGuideLink, RelTypeOptionView } from "./relTypeGuide";

// REQ-1387: each option reads its label, then the type's test.
const renderRelTypeOption = ({ option }: { option: { value: string; label: string } }) => (
  <RelTypeOptionView value={option.value} label={option.label} />
);

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
  /** ``name`` names a term the options do not hold yet (one a shape just created). */
  addGlossaryEdge: (
    fromId: number,
    toId: number,
    relType: GlossaryRelType,
    name?: string,
  ) => Promise<void>;
  /** REQ-1387 (CHOOSING RELATED_TO OPENS THE ALTERNATIVES): what the new-term form a shape starts
   * needs, and who hears that a term was created. Absent where nothing is edited. */
  newTerm?: {
    domainOptions: { value: string; label: string }[];
    domainsEnabled: boolean;
    onCreated: () => void;
  };
}

// Choosing Related to between two terms: the dialog's pair, and what "add anyway" does.
type Asked = { from: PairTerm; to: PairTerm; anyway: () => Promise<void> };
type Started = { from: PairTerm; to: PairTerm; shape: ResolveShape; name: string };

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
  newTerm,
}: GlossaryRelationshipsProps) {
  const [asked, setAsked] = useState<Asked | null>(null);
  const [started, setStarted] = useState<Started | null>(null);
  const self: PairTerm = { id: detail.id, name: detail.name };
  const termOf = (id: string): PairTerm => ({
    id: Number(id),
    name: otherTermOptions.find((o) => o.value === id)?.label ?? id,
  });
  // The edge being added, once its type is Related to and its other term is picked: ask first.
  const askAdding = (termId: string | null, relType: string | null) => {
    if (relType !== "RELATED_TO" || termId === null) return;
    const other = termOf(termId);
    const [from, to] = edgeFlipped ? [other, self] : [self, other];
    setAsked({
      from,
      to,
      anyway: () => act(() => addGlossaryEdge(from.id, to.id, "RELATED_TO")),
    });
  };
  // A shape's term created: its edge to this term joins the form, its edge to the other term is
  // written, since it touches neither this term nor the form.
  const placeEdges = async (s: Started, created: PairTerm) => {
    for (const e of shapeEdges(s.shape, s.from, s.to, created)) {
      if (e.from.id === detail.id || e.to.id === detail.id)
        await addGlossaryEdge(e.from.id, e.to.id, e.rel_type, created.name);
      else await postEdge(e.from.id, e.to.id, e.rel_type);
    }
    setStarted(null);
    setEdgeTermId(null);
    setEdgeRelType(null);
    newTerm?.onCreated();
  };
  // Retyping to Related to asks first, too; "anyway" is then the retype.
  const retype = (from: PairTerm, to: PairTerm, rel: GlossaryRelType, next: GlossaryRelType) => {
    const run = () => act(() => retypeGlossaryEdge(from.id, to.id, rel, next));
    if (next === "RELATED_TO") setAsked({ from, to, anyway: run });
    else void run();
  };
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
      onChange={(v) => {
        setEdgeTermId(v);
        askAdding(v, edgeRelType);
      }}
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
                    renderOption={renderRelTypeOption}
                    comboboxProps={{ width: 340, position: "bottom-start" }}
                    value={edge.rel_type}
                    onChange={(next) =>
                      next &&
                      next !== edge.rel_type &&
                      retype(
                        self,
                        { id: edge.term_id, name: edge.name },
                        edge.rel_type,
                        next as GlossaryRelType,
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
                    renderOption={renderRelTypeOption}
                    comboboxProps={{ width: 340, position: "bottom-start" }}
                    value={edge.rel_type}
                    onChange={(next) =>
                      next &&
                      next !== edge.rel_type &&
                      retype(
                        { id: edge.term_id, name: edge.name },
                        self,
                        edge.rel_type,
                        next as GlossaryRelType,
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
            renderOption={renderRelTypeOption}
            comboboxProps={{ width: 340, position: "bottom-start" }}
            value={edgeRelType}
            onChange={(v) => {
              setEdgeRelType(v);
              askAdding(edgeTermId, v);
            }}
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
            variant="subtle"
            aria-label={t("glossaryTab.flipEdgeLabel")}
            onClick={() => setEdgeFlipped((flipped) => !flipped)}
            data-testid="glossary-edge-flip-btn"
          >
            <ArrowLeftRight size={14} />
          </ActionIcon>
          {/* REQ-1387: how to choose a type that grounds, and what resolves a Related to. */}
          <GroundingGuideLink testId="glossary-edge-guide" />
        </Group>
      )}
      {asked && (
        <RelatedToDialog
          from={asked.from}
          to={asked.to}
          onCancel={() => {
            setAsked(null);
            setEdgeRelType(null);
          }}
          onAnyway={() => {
            const { anyway } = asked;
            setAsked(null);
            void anyway();
          }}
          onStart={(shape, name) => {
            setStarted({ from: asked.from, to: asked.to, shape, name });
            setAsked(null);
          }}
        />
      )}
      {started && newTerm && (
        <ShapeTermForm
          shape={started.shape}
          from={started.from}
          to={started.to}
          initialName={started.name}
          initialDomains={detail.domains.filter((d) =>
            newTerm.domainOptions.some((o) => o.value === d),
          )}
          domainOptions={newTerm.domainOptions}
          domainsEnabled={newTerm.domainsEnabled}
          onCreated={(created) => placeEdges(started, created)}
          onCancel={() => setStarted(null)}
        />
      )}
    </>
  );
}
