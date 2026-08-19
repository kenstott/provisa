// Copyright (c) 2026 Kenneth Stott
// Canary: d8ab0549-f116-4818-a5ed-f80c3f153554
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: admin business-glossary curation. Terms are lifecycle-managed by
// the semantic layer (created/linked and removed/deprecated with the model);
// this tab carries the human curation — rename, definitions, ref moves,
// experts, abstract terms and their typed edges.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  NavLink,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Textarea,
  Title,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Archive, ArchiveRestore, BookOpen, Plus, Sparkles, Trash2 } from "lucide-react";
import { FilterInput } from "./FilterInput";
import { HelpBubble } from "../HelpBubble";
import {
  GLOSSARY_EXPERT_KINDS,
  GLOSSARY_REL_TYPES,
  addGlossaryEdge,
  addGlossaryExpert,
  createGlossaryTerm,
  deleteGlossaryTerm,
  fetchGlossaryTerm,
  generateAllGlossaryDefinitions,
  generateGlossaryDefinition,
  generateGlossaryRelationships,
  listGlossaryTerms,
  moveGlossaryRef,
  removeGlossaryEdge,
  removeGlossaryExpert,
  retypeGlossaryEdge,
  updateGlossaryTerm,
} from "../../api/glossary";
import { DescriptionField } from "../../pages/tables/DescriptionField";
import type {
  GlossaryExpertKind,
  GlossaryRef,
  GlossaryRelType,
  GlossaryTermDetail,
  GlossaryTermSummary,
} from "../../api/glossary";

export function GlossaryTab() {
  const { t } = useTranslation();

  const [terms, setTerms] = useState<GlossaryTermSummary[]>([]);
  const [listLoading, setListLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [hideDeprecated, setHideDeprecated] = useState(false);

  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [detail, setDetail] = useState<GlossaryTermDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // Editable copies of the selected term's name/definition.
  const [name, setName] = useState("");
  const [definition, setDefinition] = useState("");
  const [exportExcluded, setExportExcluded] = useState(false);
  const [generatingDefinition, setGeneratingDefinition] = useState(false);
  const [bulkGeneratingDefinitions, setBulkGeneratingDefinitions] = useState(false);
  const [bulkGeneratingRelationships, setBulkGeneratingRelationships] = useState(false);

  const [actionError, setActionError] = useState("");

  // Add-edge form
  const [edgeTermId, setEdgeTermId] = useState<string | null>(null);
  const [edgeRelType, setEdgeRelType] = useState<string | null>(null);

  // Add-expert form
  const [expertUserId, setExpertUserId] = useState("");
  const [expertKind, setExpertKind] = useState<string>("expert");

  // New abstract term modal
  const [addOpen, setAddOpen] = useState(false);
  const [addName, setAddName] = useState("");
  const [addDefinition, setAddDefinition] = useState("");
  const [addSaving, setAddSaving] = useState(false);
  const [addError, setAddError] = useState("");

  const refreshList = useCallback(async () => {
    setListLoading(true);
    try {
      setTerms(await listGlossaryTerms(query, !hideDeprecated));
    } finally {
      setListLoading(false);
    }
  }, [query, hideDeprecated]);

  useEffect(() => {
    // Deferred so the loading flag is never set synchronously inside the effect body
    // (react-hooks/set-state-in-effect); doubles as a keystroke debounce for the search.
    const timer = window.setTimeout(() => void refreshList(), 100);
    return () => window.clearTimeout(timer);
  }, [refreshList]);

  const loadDetail = useCallback(async (termId: number) => {
    setDetailLoading(true);
    setActionError("");
    try {
      const term = await fetchGlossaryTerm(termId);
      setDetail(term);
      setName(term.name);
      setDefinition(term.definition ?? "");
      setExportExcluded(term.export_excluded);
      setEdgeTermId(null);
      setEdgeRelType(null);
      setExpertUserId("");
      setExpertKind("expert");
    } finally {
      setDetailLoading(false);
    }
  }, []);

  useEffect(() => {
    if (selectedId === null) return;
    // Deferred for the same set-state-in-effect rule as the list refresh above.
    const timer = window.setTimeout(() => void loadDetail(selectedId), 0);
    return () => window.clearTimeout(timer);
  }, [selectedId, loadDetail]);

  // Drafts a definition with the org's AI model into the editor; nothing persists
  // until the user saves it.
  const handleGenerateDefinition = async () => {
    if (detail === null) return;
    setGeneratingDefinition(true);
    setActionError("");
    try {
      const draft = await generateGlossaryDefinition(detail.id);
      if (draft) setDefinition(draft);
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setGeneratingDefinition(false);
    }
  };

  // Runs a mutation, then refreshes the detail panel and the list (names,
  // ref counts and deprecation flags all change server-side).
  const act = async (fn: () => Promise<void>, successMessage?: string) => {
    setActionError("");
    try {
      await fn();
      if (successMessage) notifications.show({ color: "green", message: successMessage });
      if (selectedId !== null) await loadDetail(selectedId);
      await refreshList();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleCreate = async () => {
    if (!addName.trim()) return;
    setAddSaving(true);
    setAddError("");
    try {
      const { id } = await createGlossaryTerm({
        name: addName.trim(),
        ...(addDefinition.trim() ? { definition: addDefinition.trim() } : {}),
      });
      notifications.show({
        color: "green",
        message: t("glossaryTab.created", { name: addName.trim() }),
      });
      setAddOpen(false);
      setAddName("");
      setAddDefinition("");
      await refreshList();
      setSelectedId(id);
    } catch (e) {
      setAddError(e instanceof Error ? e.message : String(e));
    } finally {
      setAddSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!detail) return;
    if (!window.confirm(t("glossaryTab.deleteConfirm", { name: detail.name }))) return;
    setActionError("");
    try {
      await deleteGlossaryTerm(detail.id);
      notifications.show({
        color: "green",
        message: t("glossaryTab.deleted", { name: detail.name }),
      });
      setSelectedId(null);
      setDetail(null);
      await refreshList();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleBulkGenerateDefinitions = async () => {
    setBulkGeneratingDefinitions(true);
    setActionError("");
    try {
      const { generated } = await generateAllGlossaryDefinitions();
      notifications.show({
        color: "green",
        message: t("glossaryTab.definitionsGenerated", { count: generated }),
      });
      if (selectedId !== null) await loadDetail(selectedId);
      await refreshList();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setBulkGeneratingDefinitions(false);
    }
  };

  const handleBulkGenerateRelationships = async () => {
    setBulkGeneratingRelationships(true);
    setActionError("");
    try {
      const { added } = await generateGlossaryRelationships();
      notifications.show({
        color: "green",
        message: t("glossaryTab.relationshipsGenerated", { count: added }),
      });
      if (selectedId !== null) await loadDetail(selectedId);
      await refreshList();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setBulkGeneratingRelationships(false);
    }
  };

  // Moving a term's last ref is the retire path: the server settles the losing term and
  // usually deletes it. Clear the selection in that case instead of letting the shared
  // post-mutation refresh re-read a term that no longer exists (a bare 404 in the panel).
  const handleMoveRef = async (ref: GlossaryRef, toTermId: string | null) => {
    if (toTermId === null) return;
    setActionError("");
    try {
      const { source_term_removed } = await moveGlossaryRef({
        table_id: ref.table_id,
        column_name: ref.column_name,
        to_term_id: Number(toTermId),
      });
      notifications.show({ color: "green", message: t("glossaryTab.moved") });
      if (source_term_removed) {
        setSelectedId(null);
        setDetail(null);
      } else if (selectedId !== null) {
        await loadDetail(selectedId);
      }
      await refreshList();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  };

  const relTypeOptions = GLOSSARY_REL_TYPES.map((rt) => ({
    value: rt,
    label: t(`glossaryTab.rel_${rt}`),
  }));
  // An incoming edge is stored in the other term's direction, so its picker offers the same
  // stored values under their reverse reading ("Classifies" for KIND_OF) — the curator sees
  // the sentence they are looking at, and the server still gets the forward type.
  const reverseRelTypeOptions = GLOSSARY_REL_TYPES.map((rt) => ({
    value: rt,
    label: t(`glossaryTab.rel_${rt}_reverse`, { defaultValue: t(`glossaryTab.rel_${rt}`) }),
  }));
  const kindOptions = GLOSSARY_EXPERT_KINDS.map((k) => ({
    value: k,
    label: t(`glossaryTab.kind_${k}`),
  }));
  const otherTermOptions = terms
    .filter((term) => term.id !== detail?.id)
    .map((term) => ({ value: String(term.id), label: term.name }));

  return (
    <Stack gap="md" style={{ height: "calc(100vh - 180px)", minHeight: 420 }}>
      {/* Same heading row as the other admin tabs: title, filter, actions. The list/detail split
          below it is this tab's own layout. */}
      <Group justify="space-between" wrap="wrap">
        <Group gap="xs" align="center" wrap="nowrap">
          <BookOpen size={22} style={{ display: "block", flexShrink: 0 }} />
          {/* Order 2: AdminPage suppresses its own page title for this tab, so this is the page
              heading, not a section heading. mb=0 drops the `.page h2` bottom margin, which would
              otherwise push the text up off the icon's centre line. */}
          <Title order={2} mb={0} lh={1}>
            {t("glossaryTab.title")}
          </Title>
        </Group>
        <FilterInput
          value={query}
          onChange={setQuery}
          placeholder={t("glossaryTab.searchPlaceholder")}
          testId="glossary-search"
        />
        <Group gap="xs">
          <Button
            leftSection={<Plus size={14} />}
            onClick={() => setAddOpen(true)}
            data-testid="glossary-new-btn"
          >
            {t("glossaryTab.newTerm")}
          </Button>
          {/* Most terms arrive derived, so the one thing a curator has to decide here is when
              adding a term by hand is warranted. The bubble states what the glossary is for
              rather than what the button does — the button is self-evident, the purpose is not. */}
          <HelpBubble
            title={t("glossaryTab.purposeTitle")}
            paragraphs={[t("glossaryTab.purposeBody"), t("glossaryTab.purposeAdd")]}
            ariaLabel={t("glossaryTab.purposeAria")}
            testId="glossary-purpose-help"
          />
          <Button
            variant="default"
            leftSection={<Sparkles size={14} />}
            loading={bulkGeneratingDefinitions}
            onClick={() => void handleBulkGenerateDefinitions()}
            data-testid="glossary-bulk-definitions-btn"
          >
            {t("glossaryTab.bulkGenerateDefinitions")}
          </Button>
          <Button
            variant="default"
            leftSection={<Sparkles size={14} />}
            loading={bulkGeneratingRelationships}
            onClick={() => void handleBulkGenerateRelationships()}
            data-testid="glossary-bulk-relationships-btn"
          >
            {t("glossaryTab.bulkGenerateRelationships")}
          </Button>
        </Group>
      </Group>

      <div style={{ display: "flex", gap: "0.75rem", flex: 1, minHeight: 0 }}>
        <div
          style={{
            width: 280,
            flexShrink: 0,
            display: "flex",
            flexDirection: "column",
            borderInlineEnd: "1px solid var(--border)",
          }}
          data-testid="glossary-list"
        >
          <Checkbox
            size="xs"
            px="xs"
            pb={6}
            label={t("glossaryTab.hideDeprecated")}
            checked={hideDeprecated}
            onChange={(e) => setHideDeprecated(e.currentTarget.checked)}
            data-testid="glossary-hide-deprecated"
          />
          <div style={{ flex: 1, overflow: "auto" }}>
            {listLoading && terms.length === 0 ? (
              <Group justify="center" py="md">
                <Loader size="xs" />
              </Group>
            ) : terms.length === 0 ? (
              <Text size="xs" c="dimmed" px="xs" py="sm">
                {t("glossaryTab.empty")}
              </Text>
            ) : (
              terms.map((term) => (
                <NavLink
                  key={term.id}
                  active={term.id === selectedId}
                  label={term.name}
                  onClick={() => setSelectedId(term.id)}
                  data-testid={`glossary-item-${term.id}`}
                  rightSection={
                    <Group gap={4} wrap="nowrap">
                      {term.is_abstract && (
                        <Badge size="xs" variant="light">
                          {t("glossaryTab.abstract")}
                        </Badge>
                      )}
                      {term.deprecated && (
                        <Badge size="xs" color="gray" variant="light">
                          {t("glossaryTab.deprecated")}
                        </Badge>
                      )}
                      {term.retired && (
                        <Badge size="xs" color="orange" variant="light">
                          {t("glossaryTab.retired")}
                        </Badge>
                      )}
                      {!term.live && !term.retired && !term.deprecated && (
                        <Badge size="xs" color="yellow" variant="light">
                          {t("glossaryTab.proposed")}
                        </Badge>
                      )}
                      <Badge size="xs" variant="default">
                        {term.ref_count}
                      </Badge>
                    </Group>
                  }
                />
              ))
            )}
          </div>
        </div>

        <div style={{ flex: 1, overflow: "auto" }} data-testid="glossary-detail">
          {detailLoading ? (
            <Group justify="center" py="md">
              <Loader size="xs" />
            </Group>
          ) : !detail ? (
            <Text size="sm" c="dimmed" py="sm">
              {t("glossaryTab.selectTerm")}
            </Text>
          ) : (
            <Stack gap="md" pb="md">
              {actionError && (
                <Alert color="red" data-testid="glossary-error">
                  {actionError}
                </Alert>
              )}

              {/* A term nothing consumes is the normal state of a freshly derived glossary, so
                  the panel says which of the two admission tests it still fails rather than
                  leaving the curator to infer it from a badge. */}
              {!detail.live && !detail.retired && !detail.deprecated && (
                <Alert color="yellow" data-testid="glossary-proposed">
                  {(detail.definition ?? "").trim()
                    ? t("glossaryTab.proposedUngrounded")
                    : t("glossaryTab.proposedUndefined")}
                </Alert>
              )}

              <Group align="flex-end" gap="sm">
                <TextInput
                  label={t("glossaryTab.nameLabel")}
                  value={name}
                  onChange={(e) => setName(e.currentTarget.value)}
                  style={{ flex: 1 }}
                  data-testid="glossary-name-input"
                />
                <Button
                  variant="default"
                  disabled={!name.trim() || name.trim() === detail.name}
                  onClick={() =>
                    void act(
                      () => updateGlossaryTerm(detail.id, { name: name.trim() }),
                      t("glossaryTab.updated"),
                    )
                  }
                  data-testid="glossary-rename-btn"
                >
                  {t("glossaryTab.rename")}
                </Button>
                <Tooltip
                  label={
                    detail.retired ? t("glossaryTab.unretireHint") : t("glossaryTab.retireHint")
                  }
                >
                  <Button
                    variant="light"
                    color={detail.retired ? "teal" : "orange"}
                    leftSection={
                      detail.retired ? <ArchiveRestore size={14} /> : <Archive size={14} />
                    }
                    onClick={() =>
                      void act(
                        () => updateGlossaryTerm(detail.id, { retired: !detail.retired }),
                        detail.retired
                          ? t("glossaryTab.unretireDone")
                          : t("glossaryTab.retireDone"),
                      )
                    }
                    data-testid="glossary-retire-btn"
                  >
                    {detail.retired ? t("glossaryTab.unretire") : t("glossaryTab.retire")}
                  </Button>
                </Tooltip>
                <Tooltip
                  label={t("glossaryTab.deleteDisabledHint")}
                  disabled={detail.refs.length === 0}
                >
                  <span>
                    <Button
                      color="red"
                      variant="light"
                      leftSection={<Trash2 size={14} />}
                      disabled={detail.refs.length > 0}
                      onClick={() => void handleDelete()}
                      data-testid="glossary-delete-btn"
                    >
                      {t("glossaryTab.delete")}
                    </Button>
                  </span>
                </Tooltip>
              </Group>

              <Checkbox
                label={t("glossaryTab.excludeFromExportLabel")}
                checked={exportExcluded}
                onChange={(e) => {
                  const next = e.currentTarget.checked;
                  setExportExcluded(next);
                  void act(() => updateGlossaryTerm(detail.id, { export_excluded: next }));
                }}
                data-testid="glossary-export-excluded-checkbox"
              />

              <Group align="flex-end" gap="sm">
                <Stack gap={2} style={{ flex: 1 }} data-testid="glossary-definition-input">
                  <Text size="sm" fw={500}>
                    {t("glossaryTab.definitionLabel")}
                  </Text>
                  <DescriptionField
                    value={definition}
                    onChange={setDefinition}
                    placeholder={t("glossaryTab.definitionLabel")}
                    rows={2}
                    generating={generatingDefinition}
                    onGenerate={() => void handleGenerateDefinition()}
                  />
                </Stack>
                <Button
                  variant="default"
                  disabled={definition === (detail.definition ?? "")}
                  onClick={() =>
                    void act(
                      () => updateGlossaryTerm(detail.id, { definition }),
                      t("glossaryTab.updated"),
                    )
                  }
                  data-testid="glossary-definition-save-btn"
                >
                  {t("glossaryTab.save")}
                </Button>
              </Group>

              <Title order={5}>{t("glossaryTab.refsTitle")}</Title>
              {detail.refs.length === 0 ? (
                <Text size="xs" c="dimmed">
                  {t("glossaryTab.noRefs")}
                </Text>
              ) : (
                <Table.ScrollContainer minWidth={480}>
                  <Table striped withTableBorder verticalSpacing="xs">
                    <Table.Thead>
                      <Table.Tr>
                        <Table.Th>{t("glossaryTab.colColumn")}</Table.Th>
                        <Table.Th>{t("glossaryTab.colTable")}</Table.Th>
                        <Table.Th>{t("glossaryTab.colSource")}</Table.Th>
                        <Table.Th>{t("glossaryTab.colDomain")}</Table.Th>
                        <Table.Th>{t("glossaryTab.colMove")}</Table.Th>
                      </Table.Tr>
                    </Table.Thead>
                    <Table.Tbody>
                      {detail.refs.map((ref) => (
                        <Table.Tr
                          key={`${ref.table_id}:${ref.column_name}`}
                          data-testid={`glossary-ref-${ref.table_id}-${ref.column_name}`}
                        >
                          <Table.Td>{ref.column_name}</Table.Td>
                          <Table.Td>{ref.alias || ref.table_name}</Table.Td>
                          <Table.Td>{ref.source_id}</Table.Td>
                          <Table.Td>{ref.domain_id}</Table.Td>
                          <Table.Td>
                            <Select
                              size="xs"
                              data={otherTermOptions}
                              value={null}
                              placeholder={t("glossaryTab.moveToPlaceholder")}
                              onChange={(v) => void handleMoveRef(ref, v)}
                              searchable
                              data-testid={`glossary-move-select-${ref.table_id}-${ref.column_name}`}
                            />
                          </Table.Td>
                        </Table.Tr>
                      ))}
                    </Table.Tbody>
                  </Table>
                </Table.ScrollContainer>
              )}

              <Title order={5}>{t("glossaryTab.relationshipsTitle")}</Title>
              <Stack gap={4}>
                {detail.edges_out.map((edge) => (
                  <Group
                    key={`out:${edge.term_id}:${edge.rel_type}`}
                    gap="xs"
                    data-testid={`glossary-edge-out-${edge.term_id}-${edge.rel_type}`}
                  >
                    {/* The type is part of the edge's identity, so correcting it is a retype,
                        not a delete plus an add — the curator is fixing one statement about
                        one pair of terms. */}
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
                    <Text size="sm">{edge.name}</Text>
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
                  </Group>
                ))}
                {detail.edges_in.map((edge) => (
                  <Group
                    key={`in:${edge.term_id}:${edge.rel_type}`}
                    gap="xs"
                    data-testid={`glossary-edge-in-${edge.term_id}-${edge.rel_type}`}
                  >
                    <Select
                      data={reverseRelTypeOptions}
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
                    <Text size="sm">{edge.name}</Text>
                    <Text size="sm" c="dimmed">
                      {t("glossaryTab.incoming")}
                    </Text>
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
                  </Group>
                ))}
                {detail.edges_out.length === 0 && detail.edges_in.length === 0 && (
                  <Text size="xs" c="dimmed">
                    {t("glossaryTab.noEdges")}
                  </Text>
                )}
              </Stack>
              <Group align="flex-end" gap="sm">
                <Select
                  label={t("glossaryTab.edgeRelLabel")}
                  data={relTypeOptions}
                  value={edgeRelType}
                  onChange={setEdgeRelType}
                  w={180}
                  data-testid="glossary-edge-rel-select"
                />
                <Select
                  label={t("glossaryTab.edgeTermLabel")}
                  data={otherTermOptions}
                  value={edgeTermId}
                  onChange={setEdgeTermId}
                  searchable
                  w={220}
                  data-testid="glossary-edge-term-select"
                />
                <Button
                  variant="default"
                  disabled={edgeTermId === null || edgeRelType === null}
                  onClick={() =>
                    void act(() =>
                      addGlossaryEdge(
                        detail.id,
                        Number(edgeTermId),
                        edgeRelType as GlossaryRelType,
                      ),
                    )
                  }
                  data-testid="glossary-edge-add-btn"
                >
                  {t("glossaryTab.addEdge")}
                </Button>
              </Group>

              <Title order={5}>{t("glossaryTab.expertsTitle")}</Title>
              <Stack gap={4}>
                {detail.experts.map((expert) => (
                  <Group
                    key={expert.user_id}
                    gap="xs"
                    data-testid={`glossary-expert-${expert.user_id}`}
                  >
                    <Text size="sm">{expert.user_id}</Text>
                    <Badge size="sm" variant="light">
                      {t(`glossaryTab.kind_${expert.kind}`)}
                    </Badge>
                    <ActionIcon
                      variant="subtle"
                      color="red"
                      size="sm"
                      aria-label={t("glossaryTab.removeExpert")}
                      onClick={() =>
                        void act(() => removeGlossaryExpert(detail.id, expert.user_id))
                      }
                    >
                      <Trash2 size={13} />
                    </ActionIcon>
                  </Group>
                ))}
                {detail.experts.length === 0 && (
                  <Text size="xs" c="dimmed">
                    {t("glossaryTab.noExperts")}
                  </Text>
                )}
              </Stack>
              <Group align="flex-end" gap="sm">
                <TextInput
                  label={t("glossaryTab.expertUserLabel")}
                  value={expertUserId}
                  onChange={(e) => setExpertUserId(e.currentTarget.value)}
                  w={220}
                  data-testid="glossary-expert-user-input"
                />
                <Select
                  label={t("glossaryTab.expertKindLabel")}
                  data={kindOptions}
                  value={expertKind}
                  onChange={(v) => v && setExpertKind(v)}
                  allowDeselect={false}
                  w={140}
                  data-testid="glossary-expert-kind-select"
                />
                <Button
                  variant="default"
                  disabled={!expertUserId.trim()}
                  onClick={() =>
                    void act(() =>
                      addGlossaryExpert(
                        detail.id,
                        expertUserId.trim(),
                        expertKind as GlossaryExpertKind,
                      ),
                    )
                  }
                  data-testid="glossary-expert-add-btn"
                >
                  {t("glossaryTab.addExpert")}
                </Button>
              </Group>
            </Stack>
          )}
        </div>
      </div>

      <Modal
        opened={addOpen}
        onClose={() => setAddOpen(false)}
        title={t("glossaryTab.newTermTitle")}
        size="lg"
      >
        <Stack gap="sm">
          <Text size="xs" c="dimmed">
            {t("glossaryTab.newTermHint")}
          </Text>
          <TextInput
            label={t("glossaryTab.nameLabel")}
            value={addName}
            onChange={(e) => setAddName(e.currentTarget.value)}
            data-testid="glossary-add-name-input"
            required
          />
          <Textarea
            label={t("glossaryTab.definitionLabel")}
            value={addDefinition}
            onChange={(e) => setAddDefinition(e.currentTarget.value)}
            autosize
            minRows={3}
            data-testid="glossary-add-definition-input"
          />
          {addError && (
            <Alert color="red" data-testid="glossary-add-error">
              {addError}
            </Alert>
          )}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setAddOpen(false)}>
              {t("glossaryTab.cancel")}
            </Button>
            <Button
              onClick={() => void handleCreate()}
              loading={addSaving}
              disabled={!addName.trim()}
              data-testid="glossary-add-save-btn"
            >
              {t("glossaryTab.create")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
}
