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

import { PageTitle } from "../PageTitle";
import { GlossaryNoTerm } from "./GlossaryNoTerm";
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
  Select,
  Stack,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import { useNavigate, useSearchParams } from "react-router-dom";
import { notifications } from "@mantine/notifications";
import { BadgeCheck, BookOpen, ClipboardList, Plus, Sparkles } from "lucide-react";
import { useScreenInputs } from "../../mcp/screenState";
import { OntologyToggle, OntologyView } from "../graph3d/OntologyGraph";
import { FilterInput } from "./FilterInput";
import { TermEditForm } from "./TermEditForm";
import { TermReadView } from "./TermReadView";
import { TermHeadActions } from "./TermHeadActions";
import { TermPanel } from "./TermPanel";
import { TermListBadges } from "./TermRoleBadge";
import { TermToReview, type FindingFix } from "./TermToReview";
import { TermImpactPreview } from "./TermImpactPreview";
import { defaultFindDomains, tableNames } from "./termFindings";
import { RelatedToFilter, RelatedToHint, RelatedToReviewModal } from "./relTypeGuide";
import {
  linkedTermFilters,
  matchesTermFilters,
  TERM_FILTERS,
  withTermFilters,
  type TermListFilters,
} from "./termListFilters";
import { RetireTermDialog } from "./RetireTermDialog";
import { FindTermsDialog } from "./FindTermsDialog";
import { OntologyImportsView } from "./ontologyImport/OntologyImportsView";
import { ImportsToggle } from "./ontologyImport/ImportsToggle";
import { DeprecateTermDialog } from "./DeprecateTermDialog";
import { useUnassignTag } from "../../hooks/useTagQueries";
import { refusalError } from "../../i18n/serverMessage";
import { HelpBubble } from "../HelpBubble";
import {
  GLOSSARY_EXPERT_KINDS,
  GLOSSARY_REL_TYPES,
  approveGlossaryTerm,
  createGlossaryTerm,
  deleteGlossaryTerm,
  fetchGlossaryTerm,
  GlossaryTermGone,
  generateAllGlossaryDefinitions,
  generateGlossaryRelationships,
  fetchGlossaryFindings,
  listAttributablePeople,
  type AttributablePerson,
  type GlossaryFinding,
} from "../../api/glossary";
import { useAuth } from "../../context/AuthContext";
import { useRequiredFields } from "../form/useRequiredFields";
import {
  ENTERPRISE_DOMAIN,
  onlyRelatedTo,
  orderRelTypes,
  type RelatedToReview,
} from "../../api/glossary";
import { useCapability } from "../../hooks/useCapability";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { useViewDomains } from "../../lib/viewDomains";
import type {
  GlossaryTermDetail,
  GlossaryTermSummary,
  TermGrounding,
  TermStatus,
} from "../../api/glossary";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { DetailOutOfList } from "../list/DetailOutOfList";
import { domainNarrowing, filterNarrowing, leftOutOf, type Narrowing } from "../list/leftOut";
import { usePanelDeleting } from "../list/panelActions";
import {
  readStoredBool,
  readStoredString,
  useGlossaryPanel,
  useReadableTerms,
  writeStored,
  writeStoredBool,
} from "./useGlossaryPanel";
import { assignableDomainIds } from "../../lib/internalDomains";
import { termLabel, termOptions } from "./termLabel";

export function GlossaryTab() {
  const { t } = useTranslation();
  // REQ-1590: `glossary_read` opened this page; curation is the second right. Without it the term
  // list, definitions, refs, relationships and experts all still render — a reader came here to
  // look a term up — but every control that writes is withheld rather than disabled, because the
  // endpoints behind them answer 403 and a greyed-out button says nothing about why.
  const canEdit = useCapability("glossary_rw");
  const [searchParams, setSearchParams] = useSearchParams();
  // The page reopens as it was left: its view, search and filters are per-viewer preferences kept in
  // the browser. A deep link (?term=, ?search=) decides that visit instead, and is not saved over
  // them; only the reader's own changes are.
  const [readableTerms, readTerms] = useReadableTerms(); // REQ-2260, REQ-2277 rule 9
  // REQ-1592: the org's glossary owner. It is what admits a term to the enterprise scope ("*"),
  // so the option is offered only to a holder — the server refuses it to anyone else.
  const isOrgGlossaryOwner = useCapability("org_glossary_rw");
  const { activeOrgId } = useAuth();
  const { domains: filterDomains, domainsEnabled, ensureDomainChecked } = useDomainFilter();
  // REQ-1591: the navbar selection is a VIEW preference, so it narrows the list and nothing else —
  // the server intersects it with role authority and can never widen (useViewDomains, the rule the
  // Ontology View follows too).
  const viewDomains = useViewDomains();
  const [terms, setTerms] = useState<GlossaryTermSummary[]>([]);
  // The search and domains `terms` were read for: until they are the page's, the list is not yet
  // the one in view.
  const [termsScope, setTermsScope] = useState<string | null>(null);
  const [listLoading, setListLoading] = useState(true);
  // REQ-2259, REQ-2277: a link naming a list filter (a Context Health count) decides every filter
  // that visit, and the filters stay in its URL as they change; else they reopen as left.
  const [linked] = useState(() => linkedTermFilters(searchParams));
  const [query, setQueryState] = useState(
    () => searchParams.get("search") ?? (linked ? "" : readStoredString("glossary.search")) ?? "",
  );
  const setQuery = (value: string) => {
    setQueryState(value);
    writeStored("glossary.search", value);
  };
  // REQ-2144: the list filtered by a term's status and, apart, by its grounding (REQ-1387). No
  // status picked shows every status; the choice is remembered as a comma-separated list.
  const [statusFilter, setStatusFilterState] = useState<TermStatus[]>(() =>
    linked
      ? linked.status
      : ((readStoredString("glossary.status") ?? "").split(",").filter(Boolean) as TermStatus[]),
  );
  const [groundingFilter, setGroundingFilterState] = useState<TermGrounding | null>(() =>
    linked ? linked.grounding : (readStoredString("glossary.grounding") as TermGrounding | null),
  );
  // REQ-1387: narrow the list to the terms an agent sees by name.
  const [agentVisibleOnly, setAgentVisibleOnlyState] = useState(() =>
    linked ? linked.agentVisible : readStoredBool("glossary.agentVisible", false),
  );
  // REQ-2146: narrow the list to the terms with findings.
  const [toReviewOnly, setToReviewOnlyState] = useState(() =>
    linked ? linked.toReview : readStoredBool("glossary.toReview", false),
  );
  // REQ-1387 (RELATED_TO HOLDS THE PLACE): narrow the list to the terms on a RELATED_TO edge, the
  // links that want review. A report's placeholder row opens the list with it on (REQ-1386).
  const [relatedToOnly, setRelatedToOnlyState] = useState(() =>
    linked ? linked.relatedTo : readStoredBool("glossary.relatedTo", false),
  );
  const filters: TermListFilters = {
    status: statusFilter,
    grounding: groundingFilter,
    agentVisible: agentVisibleOnly,
    toReview: toReviewOnly,
    relatedTo: relatedToOnly,
  };
  const relink = (change: Partial<TermListFilters>) => {
    if (linked !== null) {
      setSearchParams((p) => withTermFilters(p, change), { replace: true });
    }
  };
  const setStatusFilter = (value: TermStatus[]) => {
    setStatusFilterState(value);
    writeStored("glossary.status", value.join(","));
    relink({ status: value });
  };
  const setGroundingFilter = (value: TermGrounding | null) => {
    setGroundingFilterState(value);
    writeStored("glossary.grounding", value ?? "");
    relink({ grounding: value });
  };
  const setAgentVisibleOnly = (on: boolean) => {
    setAgentVisibleOnlyState(on);
    writeStoredBool("glossary.agentVisible", on);
    relink({ agentVisible: on });
  };
  const setToReviewOnly = (on: boolean) => {
    setToReviewOnlyState(on);
    writeStoredBool("glossary.toReview", on);
    relink({ toReview: on });
  };
  const setRelatedToOnly = (on: boolean) => {
    setRelatedToOnlyState(on);
    writeStoredBool("glossary.relatedTo", on);
    relink({ relatedTo: on });
  };
  // The approval being confirmed, its impact shown first (REQ-2144).
  const [approveOpen, setApproveOpen] = useState(false);
  // The RELATED_TO edges the assistant just added, each with the shapes that may resolve it.
  const [review, setReview] = useState<RelatedToReview[]>([]);

  // REQ-2277 rule 9: a term is open only while it is a row of the list shown -- read for the search
  // and the domains in view, then narrowed by the filters.
  const displayedTerms = terms.filter((term) => matchesTermFilters(term, filters));
  const listScope = JSON.stringify([query, viewDomains && [...viewDomains]]);
  // An enterprise term is in every domain's view.
  const domainNarrowed = domainNarrowing<GlossaryTermSummary>(
    t,
    viewDomains,
    ensureDomainChecked,
    (term) => term.domains.filter((d) => d !== ENTERPRISE_DOMAIN),
  );
  const readFor = new Set(terms.map((term) => term.id));
  const filterLabels: Record<keyof TermListFilters, string> = {
    status: t("glossaryTab.filterStatus"),
    grounding: t("glossaryTab.filterGrounding"),
    agentVisible: t("glossaryTab.agentVisibleOnly"),
    toReview: t("glossaryTab.toReview"),
    relatedTo: t("glossaryTab.relatedToOnly"),
  };
  const clearFilter: Record<keyof TermListFilters, () => void> = {
    status: () => setStatusFilter([]),
    grounding: () => setGroundingFilter(null),
    agentVisible: () => setAgentVisibleOnly(false),
    toReview: () => setToReviewOnly(false),
    relatedTo: () => setRelatedToOnly(false),
  };
  const termFilterNarrowings = (Object.keys(TERM_FILTERS) as (keyof TermListFilters)[]).map(
    (name) =>
      filterNarrowing<GlossaryTermSummary>(
        t,
        TERM_FILTERS[name].on(filters),
        filterLabels[name],
        (term) => TERM_FILTERS[name].keeps(term, filters),
        clearFilter[name],
      ),
  );
  const termNarrowings: (Narrowing<GlossaryTermSummary> | null)[] = [
    domainNarrowed,
    // The server reads the search: a term it did not answer, and no domain hides, the search left out.
    query.trim() === ""
      ? null
      : {
          hides: (term) => !readFor.has(term.id) && !(domainNarrowed?.hides(term) ?? false),
          reason: () => t("detailInList.bySearch", { search: query }),
          action: () => t("detailInList.clearSearch"),
          bringIn: () => setQuery(""),
        },
    // REQ-2259: each list filter, a link's included, is a reason a term is left out.
    ...termFilterNarrowings,
  ];
  const { ontology, setOntology, selectedId, setSelectedId, editing, setEditing, outOfList } =
    useGlossaryPanel(canEdit, {
      shown:
        listLoading || termsScope !== listScope
          ? null
          : displayedTerms.map((term) => String(term.id)),
      leftOut: leftOutOf(
        t,
        readableTerms,
        (term) => String(term.id),
        (term) => term.name,
        termNarrowings,
      ),
    });

  const navigate = useNavigate();
  const [detail, setDetail] = useState<GlossaryTermDetail | null>(null);
  // REQ-1327: a term's edit, status changes and delete are offered only where the server would
  // accept them: glossary_rw AND the term's own curation rule (authorship, enterprise scope,
  // domain), which the detail reports. Duplicating is a create, so glossary_rw alone offers it.
  const canCurate = canEdit && detail !== null && detail.curatable;
  const [detailLoading, setDetailLoading] = useState(false);

  // Editable copies of the selected term's name/definition.
  const [bulkGeneratingDefinitions, setBulkGeneratingDefinitions] = useState(false);
  const [bulkGeneratingRelationships, setBulkGeneratingRelationships] = useState(false);
  // REQ-2237: the ontology imports in place of the list, for a curator.
  const [imports, setImports] = useState(false);
  // REQ-2179: Find terms, and the review queue of what it proposes.
  const [findTermsOpen, setFindTermsOpen] = useState(false);
  // REQ-2115: the open term (its id is the page's `term`, useGlossaryPanel), once the glossary has
  // answered for it, and whether the Find terms dialog is open. Two domains may each hold a term of
  // one name (REQ-1591), so its domains tell it apart; its status and definition are what the panel
  // shows, for Polly to check a change it made against the term on screen.
  const shown = detail !== null && detail.id === selectedId ? detail : null;
  useScreenInputs("/context/ontology", {
    termName: shown?.name ?? null,
    termDomains: shown?.domains ?? null,
    termStatus: shown?.status ?? null,
    termDefinition: shown?.definition ?? null,
    findTerms: findTermsOpen,
  });

  const [actionError, setActionError] = useState("");
  // REQ-2198: a term's Delete works like a save. While it runs the detail is locked and the term's
  // item in the list is marked as being deleted; once it is done the detail closes, the list
  // already read again without the term. What a failed delete says is put under the panel's head.
  const deleting = usePanelDeleting(detail !== null, () => {
    setSelectedId(null);
    setDetail(null);
  });

  // REQ-1592: an expert is PICKED from the org's members, not typed. Naming an author decides who
  // may change the term from then on, so a typo would hand it to nobody.
  const [members, setMembers] = useState<AttributablePerson[]>([]);

  // New concept modal
  const [addOpen, setAddOpen] = useState(false);
  const [addName, setAddName] = useState("");
  const [addDefinition, setAddDefinition] = useState("");
  // REQ-1591: an abstract term holds no refs, so nothing derives its scope — the declaration is
  // the whole answer, and the server requires at least one domain in multi-domain mode.
  const [addDomains, setAddDomainsRaw] = useState<string[]>([]);
  const [addSaving, setAddSaving] = useState(false);
  const [addError, setAddError] = useState("");
  // REQ-1968: the name, and a domain when domains are on, are checked when Create is pressed.
  const addRequired = useRequiredFields({
    name: addName,
    ...(domainsEnabled ? { domains: addDomains } : {}),
  });

  const refreshList = useCallback(async () => {
    setListLoading(true);
    try {
      // Every status is fetched: the Status filter narrows the list in the browser.
      const domains = viewDomains && [...viewDomains];
      setTerms(await readTerms(query, domains));
      setTermsScope(JSON.stringify([query, domains]));
    } finally {
      setListLoading(false);
    }
  }, [query, viewDomains, readTerms]);

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
    const timer = window.setTimeout(
      () =>
        void loadDetail(selectedId).catch((e: unknown) => {
          // A remembered term since deleted is dropped: the panel falls back to the summary.
          if (!(e instanceof GlossaryTermGone)) throw e;
          setSelectedId(null);
        }),
      0,
    );
    return () => window.clearTimeout(timer);
  }, [selectedId, loadDetail, setSelectedId]);

  // A relationship's target term is a deep link, not just a value: click it and the detail panel
  // and the URL both switch to that term, the same as picking it from the list would.
  const navigateToTerm = useCallback((termId: number) => setSelectedId(termId), [setSelectedId]);

  // REQ-1592: the org's members are the people a term can be attributed to. Loaded once for the
  // whole surface rather than per term — the roster does not change while the page is open — and
  // only for a curator, since a reader has no picker to fill. Read from the glossary's own roster,
  // under the right that fills the picker, not the team page's, which takes user management.
  useEffect(() => {
    if (!canEdit || !activeOrgId) return;
    listAttributablePeople()
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

  // "+ New term" opens the form empty, whatever term is open or was duplicated and cancelled.
  const openNewTerm = () => {
    setAddName("");
    setAddDefinition("");
    setAddDomainsRaw([]);
    setAddError("");
    setAddOpen(true);
  };
  // Duplicate, from a term's panel: the same form filled with the term's definition and domains
  // under "<name>-copy" (editable; a name that exists is refused on save by the create). Not
  // copied: its links (columns, tables, relationships) and its reviews.
  const openDuplicateTerm = (term: GlossaryTermDetail) => {
    setAddName(`${term.name}-copy`);
    setAddDefinition(term.definition ?? "");
    setAddDomainsRaw([...term.domains]);
    setAddError("");
    setAddOpen(true);
  };

  const handleCreate = async () => {
    if (!addRequired.validate()) return;
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
      addRequired.reset();
      await refreshList();
      setSelectedId(id);
    } catch (e) {
      setAddError(e instanceof Error ? e.message : String(e));
    } finally {
      setAddSaving(false);
    }
  };

  // Resolves once the term is deleted and the list read again; rejects with the server's reason.
  const deleteTerm = async (term: GlossaryTermDetail) => {
    await deleteGlossaryTerm(term.id);
    notifications.show({
      color: "green",
      message: t("glossaryTab.deleted", { name: term.name }),
    });
    await refreshList();
  };

  const handleBulkGenerateDefinitions = async () => {
    setBulkGeneratingDefinitions(true);
    setActionError("");
    try {
      const { generated, tables_without_samples } =
        await generateAllGlossaryDefinitions(viewDomains);
      notifications.show({
        color: "green",
        message: t("glossaryTab.definitionsGenerated", { count: generated }),
      });
      // REQ-1387: name the tables the drafts read no samples from; it stays until dismissed.
      if (tables_without_samples.length > 0)
        notifications.show({
          color: "yellow",
          autoClose: false,
          message: t("glossaryTab.tablesWithoutSamples", {
            tables: tables_without_samples.join(", "),
          }),
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
      const { added, review: toResolve } = await generateGlossaryRelationships(viewDomains);
      notifications.show({
        color: "green",
        message: t("glossaryTab.relationshipsGenerated", { count: added }),
      });
      setReview(toResolve);
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
    // REQ-1387: the grounding types first, RELATED_TO last.
    () =>
      orderRelTypes(GLOSSARY_REL_TYPES).map((rt) => ({
        value: rt,
        label: t(`glossaryTab.rel_${rt}`),
      })),
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
  const otherTermOptions = termOptions(terms.filter((term) => term.id !== detail?.id));
  const statusOptions = (["draft", "approved", "deprecated", "retired"] as const).map((st) => ({
    value: st,
    label: t(`glossaryTab.status_${st}`),
  }));
  const groundingOptions = (["grounded", "grounded_pending", "not_grounded"] as const).map((g) => ({
    value: g,
    label: t(`glossaryTab.groundingValue_${g}`),
  }));

  return (
    <Stack gap="md" style={{ height: "100%", minHeight: 420 }}>
      {/* Same heading row as the other admin tabs: title, filter, actions. The list/detail split
          below it is this tab's own layout. */}
      <Group justify="space-between" wrap="wrap">
        {/* AdminPage suppresses its own page title for this tab, so this is the page heading. */}
        <PageTitle icon={BookOpen}>{t("glossaryTab.title")}</PageTitle>
        <FilterInput
          value={query}
          onChange={setQuery}
          placeholder={t("glossaryTab.searchPlaceholder")}
          testId="glossary-search"
          remember={false}
        />
        <Group gap="xs" data-toolbar>
          {canEdit && (
            <Button
              leftSection={<Plus size={14} />}
              onClick={openNewTerm}
              data-testid="glossary-new-btn"
            >
              {t("glossaryTab.newTerm")}
            </Button>
          )}
          <Tip text={t("glossaryTab.noTermHint")} named>
            <ActionIcon
              size="input-sm"
              variant={!ontology && selectedId === null ? "light" : "subtle"}
              aria-label={t("glossaryTab.noTermHint")}
              onClick={() => {
                setOntology(false);
                setImports(false);
                setSelectedId(null);
                setDetail(null);
              }}
              data-testid="glossary-stewardship-btn"
            >
              <ClipboardList size={16} aria-hidden />
            </ActionIcon>
          </Tip>
          <OntologyToggle
            open={ontology && !imports}
            onChange={(open) => {
              setImports(false);
              setOntology(open);
            }}
          />
          {canEdit && <ImportsToggle open={imports} onChange={setImports} />}
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
                  onClick={() => setFindTermsOpen(true)}
                  data-testid="glossary-find-terms-btn"
                >
                  {t("findTerms.menuItem")}
                </Menu.Item>
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
      {imports && canEdit ? (
        <OntologyImportsView
          terms={terms}
          domainOptions={addDomainOptions}
          domainsEnabled={domainsEnabled}
          defaultDomains={defaultFindDomains(viewDomains, addDomainOptions)}
        />
      ) : ontology ? (
        <OntologyView search={query} />
      ) : (
        <>
          <DetailOutOfList outOfList={outOfList} />
          <div style={{ display: "flex", gap: "0.75rem", flex: 1, minHeight: 0 }}>
            <div
              style={{
                width: 340,
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
                  label={t("glossaryTab.toReview")}
                  checked={toReviewOnly}
                  onChange={(e) => setToReviewOnly(e.currentTarget.checked)}
                  styles={{ label: { whiteSpace: "nowrap" } }}
                  data-testid="glossary-to-review-only"
                />
                <Checkbox
                  size="xs"
                  label={t("glossaryTab.agentVisibleOnly")}
                  checked={agentVisibleOnly}
                  onChange={(e) => setAgentVisibleOnly(e.currentTarget.checked)}
                  styles={{ label: { whiteSpace: "nowrap" } }}
                  data-testid="glossary-agent-visible-only"
                />
                <RelatedToFilter checked={relatedToOnly} onChange={setRelatedToOnly} />
              </Group>
              {/* Status on a line of its own: its picked statuses need the column's width. */}
              <Stack gap={6} px="xs" pb={6}>
                <Tip text={t("glossaryTab.filterStatusHint")}>
                  <div>
                    <MultiSelect
                      size="xs"
                      placeholder={
                        statusFilter.length === 0 ? t("glossaryTab.filterStatus") : undefined
                      }
                      aria-label={t("glossaryTab.filterStatus")}
                      data={statusOptions}
                      value={statusFilter}
                      onChange={(v) => setStatusFilter(v as TermStatus[])}
                      clearable
                      comboboxProps={{ withinPortal: true, width: "max-content" }}
                      data-testid="glossary-status-filter"
                    />
                  </div>
                </Tip>
                <Select
                  size="xs"
                  placeholder={t("glossaryTab.filterGrounding")}
                  aria-label={t("glossaryTab.filterGrounding")}
                  data={groundingOptions}
                  value={groundingFilter}
                  onChange={(v) => setGroundingFilter(v as TermGrounding | null)}
                  clearable
                  comboboxProps={{ withinPortal: true, width: "max-content" }}
                  data-testid="glossary-grounding-filter"
                />
              </Stack>
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
                  displayedTerms.map((term) => {
                    // REQ-2198: a term being deleted is offered for nothing.
                    const termDeleting = deleting.deleting.has(String(term.id));
                    return (
                      <NavLink
                        key={term.id}
                        active={term.id === selectedId}
                        label={termLabel(term, terms)}
                        onClick={termDeleting ? undefined : () => setSelectedId(term.id)}
                        disabled={termDeleting}
                        data-deleting={termDeleting || undefined}
                        aria-disabled={termDeleting || undefined}
                        aria-busy={termDeleting || undefined}
                        inert={termDeleting}
                        title={termDeleting ? t("common.deleting") : undefined}
                        data-testid={`glossary-item-${term.id}`}
                        // REQ-2010: a demo's tour opens a term by its name.
                        data-glossary-term={term.name}
                        rightSection={<TermListBadges term={term} />}
                      />
                    );
                  })
                )}
              </div>
            </div>

            <div
              style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}
              data-testid="glossary-detail"
            >
              {detailLoading ? (
                <Group justify="center" py="md">
                  <Loader size="xs" />
                </Group>
              ) : !detail || detail.id !== selectedId ? (
                // REQ-2259: the stewardship summary (REQ-2144) is on Context Health. A term the list
                // does not show is not open (REQ-2277 rule 9), whatever was last read.
                <GlossaryNoTerm />
              ) : (
                <TermPanel name={detail.name} host={deleting}>
                  <TermHeadActions
                    detail={detail}
                    canEdit={canEdit}
                    canCurate={canCurate}
                    act={act}
                    onEdit={() => setEditing(true)}
                    onDuplicate={() => openDuplicateTerm(detail)}
                    onApprove={() => setApproveOpen(true)}
                    onDeprecate={() => setDeprecateOpen(true)}
                    onRetire={() => setRetireOpen(true)}
                    onDelete={() => deleteTerm(detail)}
                    onTagsChanged={() => void loadDetail(detail.id, true)}
                    editing={editing}
                  >
                    {actionError && (
                      <Alert color="red" data-testid="glossary-error">
                        {actionError}
                      </Alert>
                    )}

                    {/* A draft's and a term's grounding's state hints sit beside their values
                    (TermReadView): Status, Grounding. */}
                    {/* REQ-1387 (RELATED_TO IS THE LAST RESORT): held only by RELATED_TO, which does
                    not ground, so the pair may want a term between. Guidance, never a block. */}
                    {onlyRelatedTo(detail) && <RelatedToHint />}

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
                              return canCurate
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
                              return canCurate
                                ? [
                                    {
                                      label: t("glossaryTab.findingFix_retirement_blocked"),
                                      run: () => setRetireOpen(true),
                                      testId: "glossary-finding-fix-retire",
                                    },
                                  ]
                                : [];
                            case "path_pending":
                              // REQ-1387: open each term whose approval grounds this one.
                              return (f.params.approve ? f.params.approve.split(", ") : []).flatMap(
                                (name) => {
                                  const term = terms.find((x) => x.name === name);
                                  return term
                                    ? [
                                        {
                                          label: t("glossaryTab.findingFix_path_pending", {
                                            term: name,
                                          }),
                                          run: () => navigateToTerm(term.id),
                                          testId: `glossary-finding-fix-open-${term.id}`,
                                        },
                                      ]
                                    : [];
                                },
                              );
                            case "class_and_attribute":
                            case "no_data":
                            case "approved_not_grounded":
                              return canCurate ? [edit] : [];
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
                        onTermCreated={() => void refreshList()}
                      />
                    ) : (
                      <TermReadView
                        term={detail}
                        relTypeOptions={relTypeOptions}
                        otherTermOptions={otherTermOptions}
                        relLabel={relLabel}
                        navigateToTerm={navigateToTerm}
                        canEdit={canCurate}
                        onTagsChanged={() => void loadDetail(detail.id, true)}
                      />
                    )}
                  </TermHeadActions>
                </TermPanel>
              )}
            </div>
          </div>
        </>
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
                {...addRequired.field("name")}
                value={addName}
                onChange={(e) => setAddName(e.currentTarget.value)}
                data-testid="glossary-add-name-input"
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
                {...addRequired.field("domains")}
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
                data-testid="glossary-add-save-btn"
              >
                {t("glossaryTab.create")}
              </Button>
            }
          />
        </FormLayout>
      </Modal>

      {findTermsOpen && (
        <FindTermsDialog
          terms={terms}
          domainOptions={addDomainOptions}
          domainsEnabled={domainsEnabled}
          defaultDomains={defaultFindDomains(viewDomains, addDomainOptions)}
          onClose={() => setFindTermsOpen(false)}
          onGlossaryChanged={() => void refreshList()}
        />
      )}
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
      {detail && approveOpen && (
        <Modal
          opened
          onClose={() => setApproveOpen(false)}
          title={t("glossaryTab.impactTitle_approve", { term: detail.name })}
          data-testid="glossary-approve-dialog"
        >
          <Stack gap="sm">
            <TermImpactPreview termId={detail.id} action="approve" />
            <FormFooter
              onCancel={() => setApproveOpen(false)}
              action={
                <Button
                  color="teal"
                  leftSection={<BadgeCheck size={14} />}
                  onClick={() => {
                    setApproveOpen(false);
                    void act(() => approveGlossaryTerm(detail.id), t("glossaryTab.approveDone"));
                  }}
                  data-testid="glossary-approve-confirm"
                >
                  {t("glossaryTab.approve")}
                </Button>
              }
            />
          </Stack>
        </Modal>
      )}
      <RelatedToReviewModal
        review={review}
        onClose={() => setReview([])}
        onShowTerm={(termId) => {
          setReview([]);
          navigateToTerm(termId);
        }}
      />
    </Stack>
  );
}
