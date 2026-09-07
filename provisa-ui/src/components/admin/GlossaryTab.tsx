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

import { useCallback, useEffect, useMemo, useState } from "react";
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
  MultiSelect,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Textarea,
  Title,
  Tooltip,
} from "@mantine/core";
import { useSearchParams } from "react-router-dom";
import { notifications } from "@mantine/notifications";
import {
  Archive,
  ArchiveRestore,
  BookOpen,
  Pencil,
  Plus,
  Sparkles,
  Trash2,
} from "lucide-react";
import { FilterInput } from "./FilterInput";
import { GlossaryRelationships } from "./GlossaryRelationships";
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
import { fetchOrgMembers } from "../../api/admin";
import type { OrgMember } from "../../api/admin";
import { DescriptionField } from "../../pages/tables/DescriptionField";
import { useAuth } from "../../context/AuthContext";
import { ENTERPRISE_DOMAIN } from "../../api/glossary";
import { useCapability } from "../../hooks/useCapability";
import { useDomainFilter } from "../../context/DomainFilterContext";
import type {
  GlossaryExpertKind,
  GlossaryRef,
  GlossaryTermDetail,
  GlossaryTermSummary,
} from "../../api/glossary";

// Per-viewer list-filter preferences; never reachable from other viewers or the server.
function readStoredBool(key: string, fallback: boolean): boolean {
  try {
    const raw = window.localStorage.getItem(key);
    return raw === null ? fallback : raw === "true";
  } catch {
    return fallback;
  }
}

function writeStoredBool(key: string, value: boolean): void {
  try {
    window.localStorage.setItem(key, String(value));
  } catch {
    // Private mode / storage disabled: the preference just doesn't persist this session.
  }
}

export function GlossaryTab() {
  const { t } = useTranslation();
  // REQ-1590: `glossary_read` opened this page; curation is the second right. Without it the term
  // list, definitions, refs, relationships and experts all still render — a reader came here to
  // look a term up — but every control that writes is withheld rather than disabled, because the
  // endpoints behind them answer 403 and a greyed-out button says nothing about why.
  const canEdit = useCapability("glossary_rw");
  // REQ-1592: the org's glossary owner. It is what admits a term to the enterprise scope ("*"),
  // so the option is offered only to a holder — the server refuses it to anyone else.
  const isOrgGlossaryOwner = useCapability("org_glossary_rw");
  const { activeOrgId } = useAuth();
  const { domains: filterDomains, checkedDomains, domainsEnabled } = useDomainFilter();
  // REQ-1591: the navbar selection is a VIEW preference, so it narrows the list and nothing else —
  // the server intersects it with role authority and can never widen. Held as the context's own Set
  // rather than a fresh array so its identity is stable enough to be a refresh dependency.
  const viewDomains =
    domainsEnabled && checkedDomains.size > 0 && checkedDomains.size < filterDomains.length
      ? checkedDomains
      : null;
  const [terms, setTerms] = useState<GlossaryTermSummary[]>([]);
  const [listLoading, setListLoading] = useState(true);
  const [query, setQuery] = useState("");
  // REQ-1387: checked = show that category; both default on so the list starts unfiltered. Choice
  // is a per-viewer UI preference, not server state, so it's remembered in localStorage.
  const [showDeprecated, setShowDeprecated] = useState(() => readStoredBool("glossary.showDeprecated", true));
  // Proposed (not yet live/retired/deprecated) terms are the raw output of the semantic layer's
  // derivation, not curated — unchecking this narrows the list to terms a curator has admitted.
  const [showProposed, setShowProposed] = useState(() => readStoredBool("glossary.showProposed", true));

  useEffect(() => {
    writeStoredBool("glossary.showDeprecated", showDeprecated);
  }, [showDeprecated]);

  useEffect(() => {
    writeStoredBool("glossary.showProposed", showProposed);
  }, [showProposed]);

  // REQ-1387: the selected term is mirrored to the ?term= query param, so a relationship link,
  // a shared URL, or the back/forward buttons all resolve to the same term this state does.
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedId, setSelectedId] = useState<number | null>(() => {
    const raw = searchParams.get("term");
    const parsed = raw ? Number(raw) : NaN;
    return Number.isFinite(parsed) ? parsed : null;
  });
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
  // Flip swaps which term is the "from" side of the edge being added, so the row can
  // express either {current term} {rel} {picked term} or the reverse without two forms.
  const [edgeFlipped, setEdgeFlipped] = useState(false);
  // Relationships and experts open read-only: a reader sees the plain list, and only
  // switches into the pickers/delete icons/add row by asking for edit mode explicitly.
  const [relEditMode, setRelEditMode] = useState(false);
  const [expertEditMode, setExpertEditMode] = useState(false);

  // Add-expert form. REQ-1592: the user is PICKED from the org's members, not typed. Naming an
  // author decides who may change the term from then on, so a typo would hand it to nobody.
  const [expertUserId, setExpertUserId] = useState<string | null>(null);
  const [expertKind, setExpertKind] = useState<string>("expert");
  const [members, setMembers] = useState<OrgMember[]>([]);

  // New abstract term modal
  const [addOpen, setAddOpen] = useState(false);
  const [addName, setAddName] = useState("");
  const [addDefinition, setAddDefinition] = useState("");
  // REQ-1591: an abstract term holds no refs, so nothing derives its scope — the declaration is
  // the whole answer, and the server requires at least one domain in multi-domain mode.
  const [addDomains, setAddDomainsRaw] = useState<string[]>([]);
  const [addSaving, setAddSaving] = useState(false);
  const [addError, setAddError] = useState("");

  const refreshList = useCallback(async () => {
    setListLoading(true);
    try {
      setTerms(await listGlossaryTerms(query, showDeprecated, viewDomains && [...viewDomains]));
    } finally {
      setListLoading(false);
    }
  }, [query, showDeprecated, viewDomains]);

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
      setExpertUserId(null);
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

  // Each term opens with relationships/experts collapsed to their clean read view, not
  // whatever edit mode the previous term was left in. Deferred for the same
  // set-state-in-effect rule as the list refresh above.
  useEffect(() => {
    const timer = window.setTimeout(() => {
      setRelEditMode(false);
      setExpertEditMode(false);
    }, 0);
    return () => window.clearTimeout(timer);
  }, [selectedId]);

  useEffect(() => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (selectedId === null) next.delete("term");
        else next.set("term", String(selectedId));
        return next;
      },
      { replace: true },
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps -- setSearchParams identity is stable per render, not per selection.
  }, [selectedId]);

  // A relationship's target term is a deep link, not just a value: click it and the detail panel
  // and the URL both switch to that term, the same as picking it from the list would.
  const navigateToTerm = useCallback((termId: number) => {
    setSelectedId(termId);
  }, []);

  // REQ-1592: the org's members are the people a term can be attributed to. Loaded once for the
  // whole surface rather than per term — the roster does not change while the page is open — and
  // only for a curator, since a reader has no picker to fill.
  useEffect(() => {
    if (!canEdit || !activeOrgId) return;
    fetchOrgMembers(activeOrgId)
      .then(setMembers)
      .catch((e: unknown) => setActionError(e instanceof Error ? e.message : String(e)));
  }, [canEdit, activeOrgId]);

  // Label the picker by the name a person would recognise, falling back through the identity the
  // server actually has: display name, then email, then the user id the term is keyed on.
  const memberOptions = useMemo(
    () =>
      members.map((m) => ({
        value: m.user_id,
        label: m.display_name || m.email || m.user_id,
      })),
    [members],
  );

  // REQ-1592: the enterprise scope is offered alongside the domains, to an org_glossary_rw holder
  // alone. It is exclusive — a term is either the whole org's or a named set of domains' — so
  // picking it clears the rest, and picking a domain clears it.
  const addDomainOptions = useMemo(
    () =>
      isOrgGlossaryOwner
        ? [
            { value: ENTERPRISE_DOMAIN, label: t("glossaryTab.enterpriseDomainLabel") },
            ...filterDomains,
          ]
        : filterDomains,
    [isOrgGlossaryOwner, filterDomains, t],
  );
  const setAddDomains = (next: string[]) => {
    if (next.includes(ENTERPRISE_DOMAIN)) {
      setAddDomainsRaw(
        addDomains.includes(ENTERPRISE_DOMAIN)
          ? next.filter((d) => d !== ENTERPRISE_DOMAIN)
          : [ENTERPRISE_DOMAIN],
      );
      return;
    }
    setAddDomainsRaw(next);
  };

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
        ...(domainsEnabled ? { domains: addDomains } : {}),
      });
      notifications.show({
        color: "green",
        message: t("glossaryTab.created", { name: addName.trim() }),
      });
      setAddOpen(false);
      setAddName("");
      setAddDefinition("");
      setAddDomains([]);
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
  const kindOptions = GLOSSARY_EXPERT_KINDS.map((k) => ({
    value: k,
    label: t(`glossaryTab.kind_${k}`),
  }));
  // REQ-1590: a reader sees the same sentence a curator does, without the picker that changes it.
  const relLabel = (options: { value: string; label: string }[], relType: string) =>
    options.find((o) => o.value === relType)?.label ?? relType;
  const otherTermOptions = terms
    .filter((term) => term.id !== detail?.id)
    .map((term) => ({ value: String(term.id), label: term.name }));
  // A term is "Proposed" (see the badge/alert below) exactly when it is none of live, retired or
  // deprecated — mirrored here rather than added to the summary type, since the server already
  // gives us the three booleans it derives that state from.
  const displayedTerms = showProposed
    ? terms
    : terms.filter((term) => term.live || term.retired || term.deprecated);

  return (
    <Stack gap="md" style={{ height: "100%", minHeight: 420 }}>
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
          {canEdit && (
            <Button
              leftSection={<Plus size={14} />}
              onClick={() => setAddOpen(true)}
              data-testid="glossary-new-btn"
            >
              {t("glossaryTab.newTerm")}
            </Button>
          )}
          {/* Most terms arrive derived, so the one thing a curator has to decide here is when
              adding a term by hand is warranted. The bubble states what the glossary is for
              rather than what the button does — the button is self-evident, the purpose is not. */}
          <HelpBubble
            title={t("glossaryTab.purposeTitle")}
            paragraphs={[t("glossaryTab.purposeBody"), t("glossaryTab.purposeAdd")]}
            ariaLabel={t("glossaryTab.purposeAria")}
            testId="glossary-purpose-help"
          />
          {canEdit ? (
            <>
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
            </>
          ) : (
            // REQ-1590: say which of the two rights the viewer holds, so a reader who expected to
            // curate knows a right is missing rather than that the buttons failed to render.
            <Tooltip label={t("glossaryTab.readOnlyHint")}>
              <Badge variant="light" color="gray" data-testid="glossary-read-only">
                {t("glossaryTab.readOnly")}
              </Badge>
            </Tooltip>
          )}
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
          <Group gap="md" px="xs" pb={6} wrap="nowrap">
            <Checkbox
              size="xs"
              label={t("glossaryTab.deprecated")}
              checked={showDeprecated}
              onChange={(e) => setShowDeprecated(e.currentTarget.checked)}
              data-testid="glossary-show-deprecated"
            />
            <Checkbox
              size="xs"
              label={t("glossaryTab.proposed")}
              checked={showProposed}
              onChange={(e) => setShowProposed(e.currentTarget.checked)}
              data-testid="glossary-show-proposed"
            />
          </Group>
          <div style={{ flex: 1, overflow: "auto" }}>
            {listLoading && terms.length === 0 ? (
              <Group justify="center" py="md">
                <Loader size="xs" />
              </Group>
            ) : displayedTerms.length === 0 ? (
              <Text size="xs" c="dimmed" px="xs" py="sm">
                {t("glossaryTab.empty")}
              </Text>
            ) : (
              displayedTerms.map((term) => (
                <NavLink
                  key={term.id}
                  active={term.id === selectedId}
                  label={term.name}
                  onClick={() => setSelectedId(term.id)}
                  data-testid={`glossary-item-${term.id}`}
                  rightSection={
                    <Group gap={4} wrap="nowrap">
                      {term.is_abstract && (
                        <Badge
                          size="xs"
                          variant="light"
                          color={term.grounded ? undefined : "red"}
                          data-testid={
                            term.grounded ? undefined : `glossary-dangling-${term.id}`
                          }
                        >
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
                      {!term.is_abstract && (
                        <Badge size="xs" variant="default">
                          {term.ref_count}
                        </Badge>
                      )}
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
                  readOnly={!canEdit}
                  data-testid="glossary-name-input"
                />
                {canEdit && (
                  <>
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
                      disabled={
                        detail.refs.length === 0 &&
                        detail.edges_out.length === 0 &&
                        detail.edges_in.length === 0
                      }
                    >
                      <span>
                        <Button
                          color="red"
                          variant="light"
                          leftSection={<Trash2 size={14} />}
                          disabled={
                            detail.refs.length > 0 ||
                            detail.edges_out.length > 0 ||
                            detail.edges_in.length > 0
                          }
                          onClick={() => void handleDelete()}
                          data-testid="glossary-delete-btn"
                        >
                          {t("glossaryTab.delete")}
                        </Button>
                      </span>
                    </Tooltip>
                  </>
                )}
              </Group>

              <Checkbox
                label={t("glossaryTab.excludeFromExportLabel")}
                description={t("glossaryTab.excludeFromExportHelp")}
                checked={exportExcluded}
                disabled={!canEdit}
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
                  {canEdit ? (
                    <DescriptionField
                      value={definition}
                      onChange={setDefinition}
                      placeholder={t("glossaryTab.definitionLabel")}
                      rows={2}
                      generating={generatingDefinition}
                      onGenerate={() => void handleGenerateDefinition()}
                    />
                  ) : (
                    // REQ-1590: the definition is what a reader came for, so it is shown as prose
                    // rather than in a textarea nothing can save.
                    <Text size="sm" c={definition ? undefined : "dimmed"}>
                      {definition || t("glossaryTab.hoverNoDefinition")}
                    </Text>
                  )}
                </Stack>
                {canEdit && (
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
                )}
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
                        {canEdit && <Table.Th>{t("glossaryTab.colMove")}</Table.Th>}
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
                          {canEdit && (
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
                          )}
                        </Table.Tr>
                      ))}
                    </Table.Tbody>
                  </Table>
                </Table.ScrollContainer>
              )}

              <Group gap="xs" align="center">
                <Title order={5}>{t("glossaryTab.relationshipsTitle")}</Title>
                {canEdit && (
                  <ActionIcon
                    variant="subtle"
                    size="sm"
                    aria-label={
                      relEditMode
                        ? t("glossaryTab.doneEditingRelationships")
                        : t("glossaryTab.editRelationships")
                    }
                    onClick={() => setRelEditMode((edit) => !edit)}
                    data-testid="glossary-relationships-edit-toggle"
                  >
                    <Pencil size={13} />
                  </ActionIcon>
                )}
              </Group>
              <GlossaryRelationships
                detail={detail}
                canEdit={canEdit}
                relEditMode={relEditMode}
                relTypeOptions={relTypeOptions}
                otherTermOptions={otherTermOptions}
                edgeTermId={edgeTermId}
                setEdgeTermId={setEdgeTermId}
                edgeRelType={edgeRelType}
                setEdgeRelType={setEdgeRelType}
                edgeFlipped={edgeFlipped}
                setEdgeFlipped={setEdgeFlipped}
                navigateToTerm={navigateToTerm}
                act={act}
                relLabel={relLabel}
                t={t}
                retypeGlossaryEdge={retypeGlossaryEdge}
                removeGlossaryEdge={removeGlossaryEdge}
                addGlossaryEdge={addGlossaryEdge}
              />

              <Group gap="xs" align="center">
                <Title order={5}>{t("glossaryTab.expertsTitle")}</Title>
                {canEdit && (
                  <ActionIcon
                    variant="subtle"
                    size="sm"
                    aria-label={
                      expertEditMode
                        ? t("glossaryTab.doneEditingExperts")
                        : t("glossaryTab.editExperts")
                    }
                    onClick={() => setExpertEditMode((edit) => !edit)}
                    data-testid="glossary-experts-edit-toggle"
                  >
                    <Pencil size={13} />
                  </ActionIcon>
                )}
              </Group>
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
                    {canEdit && expertEditMode && (
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
                    )}
                  </Group>
                ))}
                {detail.experts.length === 0 && (
                  <Text size="xs" c="dimmed">
                    {t("glossaryTab.noExperts")}
                  </Text>
                )}
              </Stack>
              {canEdit && expertEditMode && (
                <Group align="flex-end" gap="sm">
                  <Select
                    label={t("glossaryTab.expertUserLabel")}
                    placeholder={t("glossaryTab.expertUserPlaceholder")}
                    data={memberOptions}
                    value={expertUserId}
                    onChange={setExpertUserId}
                    searchable
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
                    disabled={!expertUserId}
                    onClick={() =>
                      void act(() =>
                        addGlossaryExpert(
                          detail.id,
                          // The button is withheld until a person is picked, so the id is present.
                          expertUserId as string,
                          expertKind as GlossaryExpertKind,
                        ),
                      )
                    }
                    data-testid="glossary-expert-add-btn"
                  >
                    {t("glossaryTab.addExpert")}
                  </Button>
                </Group>
              )}
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
          {domainsEnabled && (
            <MultiSelect
              label={t("glossaryTab.domainsLabel")}
              description={t("glossaryTab.domainsHint")}
              data={addDomainOptions}
              value={addDomains}
              onChange={setAddDomains}
              data-testid="glossary-add-domains-input"
              required
            />
          )}
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
              disabled={!addName.trim() || (domainsEnabled && addDomains.length === 0)}
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
