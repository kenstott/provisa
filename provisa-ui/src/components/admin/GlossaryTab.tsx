// Copyright (c) 2026 Kenneth Stott
// Canary: c57ff3a3-2956-4fa0-b0e0-9ba42c9e1251
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
  Menu,
  Modal,
  NavLink,
  MultiSelect,
  Stack,
  Text,
  TextInput,
  Textarea,
  Title,
} from "@mantine/core";
import { useNavigate, useSearchParams } from "react-router-dom";
import { notifications } from "@mantine/notifications";
import {
  Archive,
  ArchiveRestore,
  BookOpen,
  Hourglass,
  Pencil,
  Plus,
  Sparkles,
  Trash2,
} from "lucide-react";
import { GlossaryOntologyView, OntologyToggle } from "../graph3d/GlossaryOntologyView";
import { FilterInput } from "./FilterInput";
import { TermEditForm } from "./TermEditForm";
import { TermReadView } from "./TermReadView";
import { TermListBadges } from "./TermRoleBadge";
import { TermToReview, type FindingFix } from "./TermToReview";
import { tableNames, toReview } from "./termFindings";
import { RetireTermDialog } from "./RetireTermDialog";
import { DeprecateTermDialog } from "./DeprecateTermDialog";
import { useUnassignTag } from "../../hooks/useTagQueries";
import { refusalError } from "../../i18n/serverMessage";
import { HelpBubble } from "../HelpBubble";
import {
  GLOSSARY_EXPERT_KINDS,
  GLOSSARY_REL_TYPES,
  createGlossaryTerm,
  deleteGlossaryTerm,
  fetchGlossaryTerm,
  generateAllGlossaryDefinitions,
  generateGlossaryRelationships,
  fetchGlossaryFindings,
  listGlossaryTerms,
  restoreGlossaryTerm,
  type GlossaryFinding,
} from "../../api/glossary";
import { fetchOrgMembers } from "../../api/admin";
import type { OrgMember } from "../../api/admin";
import { useAuth } from "../../context/AuthContext";
import { ENTERPRISE_DOMAIN } from "../../api/glossary";
import { useCapability } from "../../hooks/useCapability";
import { useDomainFilter } from "../../context/DomainFilterContext";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../../api/glossary";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { getStored, setStored } from "../../lib/browserStorage";
import { assignableDomainIds } from "../../lib/internalDomains";

// Per-viewer list-filter preferences; never reachable from other viewers or the server.
function readStoredBool(key: string, fallback: boolean): boolean {
  try {
    const raw = getStored(key);
    return raw === null ? fallback : raw === "true";
  } catch {
    return fallback;
  }
}

function writeStoredBool(key: string, value: boolean): void {
  writeStored(key, String(value));
}

function readStoredString(key: string): string | null {
  try {
    return getStored(key);
  } catch {
    return null;
  }
}

function writeStored(key: string, value: string): void {
  try {
    setStored(key, value);
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
  // REQ-1387: the selected term is mirrored to the ?term= query param, so a relationship link,
  // a shared URL, or the back/forward buttons all resolve to the same term this state does.
  const [searchParams, setSearchParams] = useSearchParams();
  // The page reopens as it was left: its view, search and filters are per-viewer preferences kept in
  // the browser. A deep link (?term=, ?search=) decides that visit instead, and is not saved over
  // them; only the reader's own changes are.
  // REQ-2130: the Ontology View, in place of the term list while it is open. A ?term= link opens
  // the term, so it opens the list.
  const [ontology, setOntologyState] = useState(
    () => !searchParams.has("term") && readStoredString("glossary.view") === "ontology",
  );
  const setOntology = (open: boolean) => {
    setOntologyState(open);
    writeStored("glossary.view", open ? "ontology" : "list");
  };
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
  const [query, setQueryState] = useState(
    () => searchParams.get("search") ?? readStoredString("glossary.search") ?? "",
  );
  const setQuery = (value: string) => {
    setQueryState(value);
    writeStored("glossary.search", value);
  };
  // REQ-1387: checked = show that category; both default on so the list starts unfiltered. Choice
  // is a per-viewer UI preference, not server state, so it's remembered in localStorage.
  const [showDeprecated, setShowDeprecated] = useState(() =>
    readStoredBool("glossary.showDeprecated", true),
  );
  // Draft terms (not yet admitted) are the raw output of the semantic layer's derivation, not
  // curated — unchecking this narrows the list to terms in service or past it.
  const [showDraft, setShowDraft] = useState(() => readStoredBool("glossary.showDraft", true));
  // REQ-2146: narrow the list to the terms with findings.
  const [toReviewOnly, setToReviewOnlyState] = useState(() =>
    readStoredBool("glossary.toReview", false),
  );
  const setToReviewOnly = (on: boolean) => {
    setToReviewOnlyState(on);
    writeStoredBool("glossary.toReview", on);
  };

  useEffect(() => {
    writeStoredBool("glossary.showDeprecated", showDeprecated);
  }, [showDeprecated]);

  useEffect(() => {
    writeStoredBool("glossary.showDraft", showDraft);
  }, [showDraft]);

  const navigate = useNavigate();
  const [selectedId, setSelectedId] = useState<number | null>(() => {
    const raw = searchParams.get("term");
    const parsed = raw ? Number(raw) : NaN;
    return Number.isFinite(parsed) ? parsed : null;
  });
  const [detail, setDetail] = useState<GlossaryTermDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // Editable copies of the selected term's name/definition.
  const [bulkGeneratingDefinitions, setBulkGeneratingDefinitions] = useState(false);
  const [bulkGeneratingRelationships, setBulkGeneratingRelationships] = useState(false);

  const [actionError, setActionError] = useState("");

  // The term open as one form (TermEditForm), or read.
  const [editing, setEditing] = useState(false);

  // REQ-1592: an expert is PICKED from the org's members, not typed. Naming an author decides who
  // may change the term from then on, so a typo would hand it to nobody.
  const [members, setMembers] = useState<OrgMember[]>([]);

  // New concept modal
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

  // ``quiet``: read again in place (after a Save), so an open form keeps what it holds.
  // The open term's findings (REQ-2146), kept with the term they were read for.
  const [found, setFound] = useState<{ termId: number; findings: GlossaryFinding[] } | null>(null);
  const [retireOpen, setRetireOpen] = useState(false);
  const [deprecateOpen, setDeprecateOpen] = useState(false);
  const { unassignTag } = useUnassignTag();
  useEffect(() => {
    if (detail === null) return;
    let live = true;
    void fetchGlossaryFindings().then((rows) => {
      if (!live) return;
      // The findings list names only the terms that have any: a term it leaves out has none.
      const row = rows.find((r) => r.term_id === detail.id);
      setFound({ termId: detail.id, findings: row ? row.findings : [] });
    });
    return () => {
      live = false;
    };
  }, [detail]);
  const findings =
    found !== null && detail !== null && found.termId === detail.id ? found.findings : [];

  const loadDetail = useCallback(async (termId: number, quiet = false) => {
    if (!quiet) setDetailLoading(true);
    setActionError("");
    try {
      setDetail(await fetchGlossaryTerm(termId));
    } finally {
      if (!quiet) setDetailLoading(false);
    }
  }, []);

  // REQ-1845: Polly's glossary tools mutate terms entirely server-side (REST, never Apollo), so
  // this page — reported live as staying stale until a manual refresh — has no other way to learn
  // its list (and the currently-open term's detail, if any) is out of date.
  useEffect(() => {
    const handler = () => {
      void refreshList();
      if (selectedId !== null) void loadDetail(selectedId);
    };
    window.addEventListener("provisa:glossary-changed", handler);
    return () => window.removeEventListener("provisa:glossary-changed", handler);
  }, [refreshList, selectedId, loadDetail]);

  useEffect(() => {
    if (selectedId === null) return;
    // Deferred for the same set-state-in-effect rule as the list refresh above.
    const timer = window.setTimeout(() => void loadDetail(selectedId), 0);
    return () => window.clearTimeout(timer);
  }, [selectedId, loadDetail]);

  // Each term opens read, not in the form the previous term was left in. Deferred for the same
  // set-state-in-effect rule as the list refresh above.
  useEffect(() => {
    const timer = window.setTimeout(() => setEditing(false), 0);
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
            ...assignableDomainIds(filterDomains),
          ]
        : assignableDomainIds(filterDomains),
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

  // Runs a mutation, then refreshes the detail panel and the list (names,
  // ref counts and deprecation flags all change server-side).
  const act = async (fn: () => Promise<void>, successMessage?: string) => {
    setActionError("");
    try {
      await fn();
      if (successMessage) notifications.show({ color: "green", message: successMessage });
      // The term and the list are read again together: neither waits on the other.
      await Promise.all([selectedId !== null ? loadDetail(selectedId) : null, refreshList()]);
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

  // After the form's Save. Moving a term's last column away is the retire path: the server settles
  // the term and usually removes it, so the selection is cleared rather than re-read (a bare 404).
  // A refused change leaves the form open, the term read again under it.
  const handleSaved = async (done: boolean, removed: boolean) => {
    if (removed) {
      setSelectedId(null);
      setDetail(null);
      setEditing(false);
    }
    // The term (unless removed) and the list are read again together.
    await Promise.all([
      !removed && selectedId !== null ? loadDetail(selectedId, true) : null,
      refreshList(),
    ]);
    if (done) {
      setEditing(false);
      notifications.show({ color: "green", message: t("glossaryTab.updated") });
    }
  };

  // Kept from render to render: the term graph (REQ-2150) is drawn again only when its words change.
  const relTypeOptions = useMemo(
    () => GLOSSARY_REL_TYPES.map((rt) => ({ value: rt, label: t(`glossaryTab.rel_${rt}`) })),
    [t],
  );
  const kindOptions = GLOSSARY_EXPERT_KINDS.map((k) => ({
    value: k,
    label: t(`glossaryTab.kind_${k}`),
  }));
  // REQ-1590: a reader sees the same sentence a curator does, without the picker that changes it.
  const relLabel = useCallback(
    (options: { value: string; label: string }[], relType: string) =>
      options.find((o) => o.value === relType)?.label ?? relType,
    [],
  );
  const otherTermOptions = terms
    .filter((term) => term.id !== detail?.id)
    .map((term) => ({ value: String(term.id), label: term.name }));
  const displayedTerms = (
    showDraft ? terms : terms.filter((term) => term.status !== "draft")
  ).filter((term) => !toReviewOnly || toReview(term));

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
        <Group gap="xs" data-toolbar>
          {canEdit && (
            <Button
              leftSection={<Plus size={14} />}
              onClick={() => setAddOpen(true)}
              data-testid="glossary-new-btn"
            >
              {t("glossaryTab.newTerm")}
            </Button>
          )}
          <OntologyToggle open={ontology} onChange={setOntology} />
          {canEdit ? (
            // The AI drafts, both behind one sparkle: relationships between the terms, and the
            // definitions the terms lack.
            <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
              <Menu.Target>
                <Tip text={t("glossaryTab.generateMenu")} named>
                  <ActionIcon
                    size="input-sm"
                    variant="subtle"
                    loading={bulkGeneratingDefinitions || bulkGeneratingRelationships}
                    aria-label={t("glossaryTab.generateMenu")}
                    data-testid="glossary-generate-menu"
                  >
                    <Sparkles size={16} aria-hidden />
                  </ActionIcon>
                </Tip>
              </Menu.Target>
              <Menu.Dropdown>
                <Menu.Item
                  onClick={() => void handleBulkGenerateRelationships()}
                  data-testid="glossary-bulk-relationships-btn"
                >
                  {t("glossaryTab.bulkGenerateRelationships")}
                </Menu.Item>
                <Menu.Item
                  onClick={() => void handleBulkGenerateDefinitions()}
                  data-testid="glossary-bulk-definitions-btn"
                >
                  {t("glossaryTab.bulkGenerateDefinitions")}
                </Menu.Item>
              </Menu.Dropdown>
            </Menu>
          ) : (
            // REQ-1590: say which of the two rights the viewer holds, so a reader who expected to
            // curate knows a right is missing rather than that the buttons failed to render.
            <Tip text={t("glossaryTab.readOnlyHint")}>
              <Badge variant="light" color="gray" data-testid="glossary-read-only">
                {t("glossaryTab.readOnly")}
              </Badge>
            </Tip>
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
        </Group>
      </Group>

      {/* REQ-2130: the Ontology View in place of the list, read and drawn once it is opened. */}
      {ontology ? (
        <GlossaryOntologyView search={query} />
      ) : (
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
            {/* Each filter's label on one line; the filters wrap where a language's labels are long. */}
            <Group gap="sm" px="xs" pb={6} wrap="wrap">
              <Checkbox
                size="xs"
                label={t("glossaryTab.deprecated")}
                checked={showDeprecated}
                onChange={(e) => setShowDeprecated(e.currentTarget.checked)}
                styles={{ label: { whiteSpace: "nowrap" } }}
                data-testid="glossary-show-deprecated"
              />
              <Checkbox
                size="xs"
                label={t("glossaryTab.status_draft")}
                checked={showDraft}
                onChange={(e) => setShowDraft(e.currentTarget.checked)}
                styles={{ label: { whiteSpace: "nowrap" } }}
                data-testid="glossary-show-draft"
              />
              <Checkbox
                size="xs"
                label={t("glossaryTab.toReview")}
                checked={toReviewOnly}
                onChange={(e) => setToReviewOnly(e.currentTarget.checked)}
                styles={{ label: { whiteSpace: "nowrap" } }}
                data-testid="glossary-to-review-only"
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
                    // REQ-2010: a demo's tour opens a term by its name.
                    data-glossary-term={term.name}
                    rightSection={<TermListBadges term={term} />}
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
                {detail.status === "draft" && (
                  <Alert color="yellow" data-testid="glossary-draft">
                    {(detail.definition ?? "").trim()
                      ? t("glossaryTab.draftUngrounded")
                      : t("glossaryTab.draftUndefined")}
                  </Alert>
                )}

                {canEdit && !editing && (
                  // Edit opens the whole term as one form; Retire and Delete act at once.
                  <Group gap="xs" data-toolbar>
                    <Button
                      variant="default"
                      leftSection={<Pencil size={14} />}
                      onClick={() => setEditing(true)}
                      data-testid="glossary-edit-btn"
                    >
                      {t("glossaryTab.edit")}
                    </Button>
                    {/* REQ-2144: each status change is an action with a reason. A live term can be
                        deprecated; any term in service retired; a deprecation or a retirement
                        undone. */}
                    {detail.status === "live" && (
                      <Tip text={t("glossaryTab.deprecateHint")}>
                        <Button
                          variant="light"
                          color="gray"
                          leftSection={<Hourglass size={14} />}
                          onClick={() => setDeprecateOpen(true)}
                          data-testid="glossary-deprecate-btn"
                        >
                          {t("glossaryTab.deprecate")}
                        </Button>
                      </Tip>
                    )}
                    {detail.status !== "retired" && (
                      <Tip text={t("glossaryTab.retireHint")}>
                        <Button
                          variant="light"
                          color="orange"
                          leftSection={<Archive size={14} />}
                          onClick={() => setRetireOpen(true)}
                          data-testid="glossary-retire-btn"
                        >
                          {t("glossaryTab.retire")}
                        </Button>
                      </Tip>
                    )}
                    {(detail.status === "deprecated" || detail.status === "retired") && (
                      <Tip text={t("glossaryTab.unretireHint")}>
                        <Button
                          variant="light"
                          color="teal"
                          leftSection={<ArchiveRestore size={14} />}
                          onClick={() =>
                            void act(
                              () => restoreGlossaryTerm(detail.id),
                              t("glossaryTab.unretireDone"),
                            )
                          }
                          data-testid="glossary-restore-btn"
                        >
                          {t("glossaryTab.unretire")}
                        </Button>
                      </Tip>
                    )}
                    <Tip
                      text={
                        detail.refs.length === 0 &&
                        detail.edges_out.length === 0 &&
                        detail.edges_in.length === 0
                          ? null
                          : t("glossaryTab.deleteDisabledHint")
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
                    </Tip>
                  </Group>
                )}

                {!editing && (
                  <TermToReview
                    findings={findings}
                    fixesOf={(f): FindingFix[] => {
                      const edit: FindingFix = {
                        label: t("glossaryTab.edit"),
                        run: () => setEditing(true),
                        testId: "glossary-finding-fix-edit",
                      };
                      switch (f.kind) {
                        case "unlinked_tables":
                          // Same-entity relationships are declared on the Relationships page,
                          // found there by the tables' names.
                          return [
                            {
                              label: t("glossaryTab.findingFix_unlinked_tables"),
                              run: () =>
                                navigate(
                                  `/relationships?search=${encodeURIComponent(tableNames(f.params.tables))}`,
                                ),
                              testId: "glossary-finding-fix-same-entity",
                            },
                          ];
                        case "tag_wrong_kind":
                          return canEdit
                            ? f.params.tags.split(", ").map((tag) => ({
                                label: t("glossaryTab.findingUnmark", { tag }),
                                run: () =>
                                  void act(async () => {
                                    const r = await unassignTag({
                                      tagId: tag,
                                      objectType: "term",
                                      termId: detail.id,
                                    });
                                    if (!r.success) throw refusalError(r);
                                  }),
                                testId: `glossary-finding-fix-unmark-${tag}`,
                              }))
                            : [];
                        case "retirement_blocked":
                          return canEdit
                            ? [
                                {
                                  label: t("glossaryTab.findingFix_retirement_blocked"),
                                  run: () => setRetireOpen(true),
                                  testId: "glossary-finding-fix-retire",
                                },
                              ]
                            : [];
                        case "class_and_attribute":
                        case "no_data":
                          return canEdit ? [edit] : [];
                      }
                    }}
                  />
                )}

                {editing ? (
                  <TermEditForm
                    term={detail}
                    otherTermOptions={otherTermOptions}
                    relTypeOptions={relTypeOptions}
                    kindOptions={kindOptions}
                    memberOptions={memberOptions}
                    domainOptions={addDomainOptions.map((o) =>
                      typeof o === "string" ? { value: o, label: o } : o,
                    )}
                    domainsEnabled={domainsEnabled}
                    relLabel={relLabel}
                    navigateToTerm={navigateToTerm}
                    onSaved={handleSaved}
                    onCancel={() => setEditing(false)}
                  />
                ) : (
                  <TermReadView
                    term={detail}
                    relTypeOptions={relTypeOptions}
                    otherTermOptions={otherTermOptions}
                    relLabel={relLabel}
                    navigateToTerm={navigateToTerm}
                  />
                )}
              </Stack>
            )}
          </div>
        </div>
      )}

      <Modal
        opened={addOpen}
        onClose={() => setAddOpen(false)}
        title={t("glossaryTab.newTermTitle")}
        size="lg"
      >
        <FormLayout>
          <FormField size="full">
            <Text size="xs" c="dimmed">
              {t("glossaryTab.newTermHint")}
            </Text>
          </FormField>
          <FormIdentifiers>
            <FormField size="medium">
              <TextInput
                label={t("glossaryTab.nameLabel")}
                value={addName}
                onChange={(e) => setAddName(e.currentTarget.value)}
                data-testid="glossary-add-name-input"
                required
              />
            </FormField>
          </FormIdentifiers>
          <FormField size="full">
            <Textarea
              label={t("glossaryTab.definitionLabel")}
              value={addDefinition}
              onChange={(e) => setAddDefinition(e.currentTarget.value)}
              autosize
              minRows={3}
              data-testid="glossary-add-definition-input"
            />
          </FormField>
          {domainsEnabled && (
            <FormField size="long" help={t("glossaryTab.domainsHint")}>
              <MultiSelect
                label={t("glossaryTab.domainsLabel")}
                data={addDomainOptions}
                value={addDomains}
                onChange={setAddDomains}
                data-testid="glossary-add-domains-input"
                required
              />
            </FormField>
          )}
          {addError && (
            <FormField size="full">
              <Alert color="red" data-testid="glossary-add-error">
                {addError}
              </Alert>
            </FormField>
          )}
          {/* Create keeps its word, first; Cancel follows. */}
          <FormFooter
            onCancel={() => setAddOpen(false)}
            action={
              <Button
                onClick={() => void handleCreate()}
                loading={addSaving}
                disabled={!addName.trim() || (domainsEnabled && addDomains.length === 0)}
                data-testid="glossary-add-save-btn"
              >
                {t("glossaryTab.create")}
              </Button>
            }
          />
        </FormLayout>
      </Modal>

      {detail && deprecateOpen && (
        <DeprecateTermDialog
          term={detail}
          onClose={() => setDeprecateOpen(false)}
          onDeprecated={() => {
            setDeprecateOpen(false);
            // Deprecated already: the term and the list are read again, and the reader told.
            void act(async () => undefined, t("glossaryTab.deprecateDone"));
          }}
        />
      )}
      {detail && retireOpen && (
        <RetireTermDialog
          term={detail}
          terms={terms}
          onClose={() => setRetireOpen(false)}
          onRetired={() => {
            setRetireOpen(false);
            // Retired already: the term and the list are read again, and the reader told.
            void act(async () => undefined, t("glossaryTab.retireDone"));
          }}
        />
      )}
    </Stack>
  );
}
