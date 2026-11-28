// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { PageTitle } from "../components/PageTitle";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useScreenInputs } from "../mcp/screenState";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Group, Table, Text } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Plus, Package } from "lucide-react";
import { useDomains, useTables, useRoles, useRelationships } from "../hooks/useAdminQueries";
import { useOwnerField } from "../hooks/useOwnerField";
import {
  useCreateDataProduct,
  useUpdateDataProduct,
  useDeleteDataProduct,
  useDeleteDataProducts,
  useSetDataProductMembers,
  useDataProductFindings,
  useDataProductStatus,
} from "../hooks/useDataProductMutations";
import { ProductStatusBadge } from "./data-products/ProductStatusBadge";
import { ProductSyncCell } from "./data-products/ProductSyncStatus";
import {
  SYNC_ORDER,
  syncLabel,
  syncState,
  useProductSyncStatus,
} from "./data-products/productSync";
import type { DataProduct } from "../types/admin";
import { fetchActions, type TrackedFunction } from "../api/actions";
import { FilterInput } from "../components/admin/FilterInput";
import { MultiSelect } from "../components/MultiSelect";
import { TagControl } from "../components/TagControl";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useTagAssignments } from "../hooks/useTagQueries";
import { DataProductDetailPanel } from "./data-products/DataProductDetailPanel";
import { isProductOwner } from "./data-products/productOwner";
import { OwnerResolutionIcon } from "../components/OwnerResolution";
import { useCapability } from "../hooks/useCapability";
import { fetchProductLineage, type ProductLineage } from "../api/lineage";
import { catalogTableName, lineageRelationLabel, relationName } from "../naming";
import { DataProductFormCard } from "./data-products/DataProductFormCard";
import { useMetaProducts, useMetaReader, useProductCore } from "./data-products/useProductCore";
import { ProductMapButton } from "../components/graph3d/OntologyGraph";
import { EMPTY_FORM, type DataProductForm } from "./data-products/types";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { useRememberedDetail } from "../components/list/useDetailParam";
import {
  detailListOf,
  domainNarrowing,
  filterNarrowing,
  searchNarrowing,
  shownBy,
} from "../components/list/leftOut";
import { PageLoading } from "../components/PageLoading";
import { SplitPane } from "../components/list/SplitPane";
import { useListSelection } from "../components/list/listSelection";
import { ListBulkBar } from "../components/list/ListBulkBar";
import type { SharingProduct } from "./data-products/tabs/RelatedTab";
import { getStored, removeStored, setStored } from "../lib/browserStorage";
import { useAuth } from "../context/AuthContext";
import { profileDialogInputs } from "./tables/useProfileRunsParam";
import { heldRoles } from "../lib/roles";
import { PageActions } from "../components/PageActions";
import { PageCommand } from "../components/PageCommand";

// Exported for the tour, which clears it so the page mounts collapsed and its click on the first
// row deterministically EXPANDS rather than toggling whatever a prior visit left open.
// REQ-2146: the list narrowed to the products with findings, remembered.
const TO_REVIEW_STORAGE_KEY = "provisa.data_products.toReview";

// REQ-1634: top-level data-products management page (list / create / edit / delete).
// Follows the MetricsPage detail-then-edit pattern (REQ-1323) — row click expands the
// detail panel; Edit/Delete live inside it and edit swaps the panel for the inline form.
/** REQ-2219 (amended 2026-11-24): a product's owners as its Owner field names them, people and
 * roles as entered: what the list shows, sorts, groups and searches by. */
const ownerNames = (p: DataProduct) => p.namedOwners.map((o) => o.principal);

export function DataProductsPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [search, setSearch] = useState(() => searchParams.get("search") ?? "");
  const updateSearch = (v: string) => {
    setSearch(v);
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        if (v) n.set("search", v);
        else n.delete("search");
        return n;
      },
      { replace: true },
    );
  };
  // REQ-2127: the list is data_products_meta, read as the user through the governed pipeline.
  const products = useMetaProducts();
  const dataProducts = useMemo(() => products.data ?? [], [products.data]);
  const { loading, error } = products;
  const { domains } = useDomains();
  const { tables, refetch: refetchTables } = useTables(); // REQ-1634: member-table list + assignment
  const [functions, setFunctions] = useState<TrackedFunction[]>([]); // REQ-1634: member-command list + assignment
  const { roles } = useRoles();
  // The model's relationships: each dataset's associations in the Related tab.
  const { relationships } = useRelationships();
  const roleOptions = roles.map((r) => r.id);
  // REQ-2219 (amended 2026-11-23): the roles the Owner field may name, those holding
  // data_product_owner.
  const ownerRoleOptions = roles
    .filter((r) => r.effectiveCapabilities.includes("data_product_owner"))
    .map((r) => r.id);
  const readOwnerField = useOwnerField();
  // The product whose Owner field, as it names people and roles, the open form holds. Until it
  // is read, a save leaves the field as held.
  const [ownerFieldFor, setOwnerFieldFor] = useState<string | null>(null);
  const { createDataProduct, loading: creatingProduct } = useCreateDataProduct();
  const { updateDataProduct, loading: updatingProduct } = useUpdateDataProduct();
  const saving = creatingProduct || updatingProduct;
  const { deleteDataProduct } = useDeleteDataProduct();
  const { setDataProductMembers } = useSetDataProductMembers();
  // REQ-1634: New/Edit/Delete and the table picker require the write right; a
  // data_product_read-only caller sees the list and detail view only.
  const canEdit = useCapability("data_product_rw");
  const { selectedRoles, availableRoles, actingRoles, userId } = useAuth();
  const held = useMemo(
    () => heldRoles(selectedRoles, availableRoles),
    [selectedRoles, availableRoles],
  );
  const viewer = useMemo(() => ({ held, acting: actingRoles }), [held, actingRoles]);
  // REQ-2219 (amended 2026-11-23): the signed-in person is one of the product's owners.
  const namedOwner = (p: DataProduct) => userId !== null && p.owners.includes(userId);
  // REQ-2188: a holder of data_product_rw sees the owner's view of every product, its read rights
  // capping what is in it (REQ-2152). REQ-2160: and so does an owner, or a reader holding the
  // team role.
  const ownsProduct = (p: DataProduct) =>
    canEdit || namedOwner(p) || (p.teamRole != null && held.has(p.teamRole));
  // REQ-2188: RW that comes only from an Owner field reaches the products naming the caller; the
  // store's row rule is what enforces it, and this only keeps Edit off the others. RW of a role's
  // own (or an ancestor's) is offered every product.
  const rwOfItsOwn = availableRoles.some(
    (r) => held.has(r.id) && r.capabilities?.includes("data_product_rw"),
  );
  const mayEdit = (p: DataProduct) => canEdit && (rwOfItsOwn || namedOwner(p));
  // REQ-2146: each product's findings, and the list narrowed to the products that have any.
  const productFindings = useDataProductFindings();
  const statusActions = useDataProductStatus();
  const [toReviewOnly, setToReviewOnlyState] = useState(() => {
    try {
      return getStored(TO_REVIEW_STORAGE_KEY) === "true";
    } catch {
      return false;
    }
  });
  const setToReviewOnly = (on: boolean) => {
    setToReviewOnlyState(on);
    try {
      if (on) setStored(TO_REVIEW_STORAGE_KEY, "true");
      else removeStored(TO_REVIEW_STORAGE_KEY);
    } catch {
      // localStorage unavailable (private mode, quota) — the filter just won't persist
    }
  };
  // REQ-2180: the list faceted by the products' tags, parameterized ones included (residency:eu).
  const [tagFilter, setTagFilter] = useState<string[]>([]);
  const { tagAssignments } = useTagAssignments();
  const productTags = useMemo(() => {
    const byProduct = new Map<string, Set<string>>();
    for (const a of tagAssignments) {
      if (a.objectType !== "product" || !a.productId) continue;
      const tags = byProduct.get(a.productId) ?? new Set<string>();
      tags.add(a.tagId);
      byProduct.set(a.productId, tags);
    }
    return byProduct;
  }, [tagAssignments]);
  const tagOptions = useMemo(
    () =>
      [...new Set([...productTags.values()].flatMap((tags) => [...tags]))]
        .sort()
        .map((tag) => ({ id: tag, label: tag })),
    [productTags],
  );
  // REQ-1659 (amended 2026-11-28): the list is narrowed by the navbar's Domains filter, which on
  // this page lists the domains of the products listed -- a domain outside the role included,
  // since a listing is discovery and offers decide access -- with a selection of its own.
  const { setProductDomains, checkedProductDomains, ensureProductDomainChecked } =
    useDomainFilter();
  const productDomainIds = useMemo(
    () => [...new Set(dataProducts.map((p) => p.domainId))].sort(),
    [dataProducts],
  );
  const productDomainKey = productDomainIds.join(",");
  useEffect(() => {
    if (products.data === null) return;
    setProductDomains(productDomainIds);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed by the domain SET (the joined ids), not the array identity
  }, [productDomainKey, products.data === null, setProductDomains]);
  const [creating, setCreating] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [form, setForm] = useState<DataProductForm>({ ...EMPTY_FORM });
  const [selectedTableIds, setSelectedTableIds] = useState<string[]>([]);
  const [selectedFunctionNames, setSelectedFunctionNames] = useState<string[]>([]);
  // REQ-1659, REQ-2241: `?product=<id>` is the open product's panel -- the deep link a catalog
  // listing (Snowflake Horizon's documentation box, Analytics Hub) carries back to this page.
  // REQ-2260: a visit without one reopens the product left open.
  // REQ-2277 rule 9: a product is open only while the list -- the search and every facet -- shows
  // its row.
  const syncStatus = useProductSyncStatus();
  const productNarrowings = useMemo(
    () => [
      searchNarrowing<DataProduct>(
        t,
        search,
        () => updateSearch(""),
        // Every text a product's row shows.
        (p) => [
          p.id,
          p.domainId,
          p.name,
          ownerNames(p).join(", "),
          t(`dataProductsTab.status_${p.status}`),
          syncLabel(syncStatus.statuses[p.id] ?? [], t),
          p.purpose,
        ],
      ),
      filterNarrowing<DataProduct>(
        t,
        toReviewOnly,
        t("dataProductsTab.toReview"),
        (p) => productFindings.findings.has(p.id),
        () => setToReviewOnly(false),
      ),
      filterNarrowing<DataProduct>(
        t,
        tagFilter.length > 0,
        t("dataProductsTab.tagFacetLabel"),
        (p) => tagFilter.every((tag) => productTags.get(p.id)?.has(tag) === true),
        () => setTagFilter([]),
      ),
      domainNarrowing<DataProduct>(
        t,
        checkedProductDomains.size > 0 ? checkedProductDomains : null,
        ensureProductDomainChecked,
        (p) => [p.domainId],
      ),
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the setters are the page's own, fixed for its life; updateSearch and setToReviewOnly are redefined each render but act alike
    [
      t,
      search,
      syncStatus.statuses,
      toReviewOnly,
      productFindings.findings,
      tagFilter,
      productTags,
      checkedProductDomains,
    ],
  );
  const detail = useRememberedDetail(
    "products",
    detailListOf(
      t,
      products.data,
      (p) => p.id,
      (p) => p.name,
      productNarrowings,
    ),
  );
  const expanded = detail.id;
  // REQ-2115: Polly's read_screen reads the page's main state from here. A product is named only
  // once the list holds it, so a link to one that does not exist reads as no product.
  useScreenInputs("/data-products", {
    product: dataProducts.some((p) => p.id === expanded) ? expanded : null,
    // REQ-2277: a dataset's profile runs dialog, as the URL names it, once a product is open.
    ...profileDialogInputs(expanded !== null ? searchParams : null),
    table: expanded !== null ? searchParams.get("table") : null,
    search,
    toReviewOnly,
    tags: tagFilter,
    domains: [...checkedProductDomains],
    creating,
    editing: editingId,
  });
  const setExpanded = (id: string | null) => {
    // REQ-2241: opening and closing a product are history entries; Back undoes them.
    if (id) detail.open(id);
    else detail.close();
  };
  // REQ-2127: the expanded product's panel core; read again after the product is saved.
  const [coreVersion, setCoreVersion] = useState(0);
  const core = useProductCore(expanded, coreVersion);
  // The expanded product's lineage and contracts are read again only when its members can have
  // changed -- a save, or a retirement that moves them to the successor. Every other status change
  // leaves both as they were, so it does not read them again.
  const [membersVersion, setMembersVersion] = useState(0);
  const reader = useMetaReader();
  const [msg, setMsg] = useState("");
  // The Owner field's people search failed: said where the form says what went wrong.
  const onPeopleError = useCallback(
    (e: unknown) => setMsg(e instanceof Error ? e.message : String(e)),
    [setMsg],
  );
  // REQ-2156: the expanded product's lineage, cut to what the reader may read; read again after
  // the product is saved.
  const [lineageGraph, setLineageGraph] = useState<ProductLineage | null>(null);
  const [lineageLoading, setLineageLoading] = useState(false);
  const [lineageError, setLineageError] = useState("");

  useEffect(() => {
    // REQ-1634: command <-> data product membership lives on the (REST-served) Function row.
    fetchActions()
      .then((actions) => setFunctions(actions.functions))
      .catch(() => setFunctions([]));
  }, []);

  useEffect(() => {
    if (expanded === null) return;
    let cancelled = false;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       flips the loading flag synchronously before the async fetch starts */
    setLineageLoading(true);
    setLineageGraph(null);
    setLineageError("");
    fetchProductLineage(expanded)
      .then((g) => {
        if (!cancelled) setLineageGraph(g);
      })
      .catch((e: Error) => {
        if (!cancelled) setLineageError(e.message);
      })
      .finally(() => {
        if (!cancelled) setLineageLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [expanded, membersVersion]);

  const domainOptions = domains.map((d) => d.id);
  // REQ-1634: a table is in any number of products; its membership is the list of them.
  const memberTables = (id: string) => tables.filter((tb) => tb.productIds.includes(id));
  // A lineage relation as a reader is shown it (naming.lineageRelationLabel).
  const relationLabel = useMemo(() => {
    const catalog = new Map(tables.map((tb) => [relationName(tb), catalogTableName(tb)]));
    return (relation: string) => lineageRelationLabel(relation, catalog);
  }, [tables]);
  const memberFunctions = (id: string) =>
    functions.filter((fn) => (fn.productIds ?? []).includes(id));

  const openCreate = () => {
    setFormFocus(undefined);
    setEditingId(null);
    setForm({ ...EMPTY_FORM });
    setSelectedTableIds([]);
    setSelectedFunctionNames([]);
    setMsg("");
    setCreating(true);
  };

  // The field the edit form opens on, where a finding's fix sent the reader.
  const [formFocus, setFormFocus] = useState<"supportContact" | undefined>(undefined);
  const openEdit = (p: DataProduct, focus?: "supportContact") => {
    setFormFocus(focus);
    setCreating(false);
    setEditingId(p.id);
    setExpanded(p.id);
    setOwnerFieldFor(null);
    readOwnerField("data_product", p.id)
      .then((field) => {
        setForm((f) =>
          f.id === p.id || f.id === `${p.id}-copy`
            ? { ...f, owners: field.people, ownerRoles: field.roles }
            : f,
        );
        setOwnerFieldFor(p.id);
      })
      .catch(onPeopleError);
    setForm({
      id: p.id,
      domainId: p.domainId,
      name: p.name,
      // Its owners as its Owner field names them, read above; the product's owners (p.owners)
      // count every holder of a role named, who are not the field's.
      owners: [],
      ownerRoles: [],
      teamRole: p.teamRole ?? "",
      purpose: p.purpose,
      limitations: p.limitations,
      usage: p.usage,
      version: p.version ?? "",
      support: p.support ?? "",
      supportContact: p.supportContact ?? "",
      // The editor works on Record<string, string>; ODPS customProperties values are
      // arbitrary JSON, so a non-string existing value is rendered as its string form.
      customProperties: Object.fromEntries(
        Object.entries(p.customProperties ?? {}).map(([k, v]) => [k, String(v)]),
      ),
    });
    setSelectedTableIds(memberTables(p.id).map((tb) => String(tb.id)));
    setSelectedFunctionNames(memberFunctions(p.id).map((fn) => fn.name));
    setMsg("");
  };

  // Duplicate, from a product's panel head: the create form filled with the product's definition
  // and its member tables and commands, under "<id>-copy" (editable; an id that exists is refused
  // on save by the create). Not copied: its consumers, contracts, offers and bids.
  const openDuplicate = (p: DataProduct) => {
    openEdit(p);
    setEditingId(null);
    setForm((f) => ({ ...f, id: `${p.id}-copy` }));
    setCreating(true);
  };

  const closeForm = () => {
    setCreating(false);
    setEditingId(null);
    setMsg("");
  };

  const handleSave = async () => {
    setMsg("");
    // A new product is created; an open one is updated (a create refuses an id that exists).
    const save = editingId === null ? createDataProduct : updateDataProduct;
    const result = await save({
      id: form.id.trim(),
      domainId: form.domainId.trim(),
      name: form.name.trim(),
      // An edited product's Owner field is sent once it has been read; before that it is left
      // as held.
      ...(editingId === null || ownerFieldFor === editingId
        ? { owners: form.owners, ownerRoles: form.ownerRoles }
        : { owners: null, ownerRoles: null }),
      teamRole: form.teamRole.trim() || null,
      purpose: form.purpose.trim(),
      limitations: form.limitations.trim(),
      usage: form.usage.trim(),
      version: form.version.trim() || null,
      support: form.support.trim() || null,
      supportContact: form.supportContact.trim() || null,
      customProperties: form.customProperties,
    });
    if (!result.success) {
      setMsg(result.message || t("dataProductsTab.saveFailed"));
      return;
    }
    // REQ-1634: membership is the product's own: the multi-select is saved as the product's whole
    // member list, only when it differs from the current one. A checker table is a member as any
    // other is (REQ-1443).
    const productId = form.id.trim();
    const sameSet = (a: string[], b: string[]) =>
      a.length === b.length && a.every((x) => b.includes(x));
    const currentTables = memberTables(productId).map((tb) => String(tb.id));
    const currentFns = memberFunctions(productId).map((fn) => fn.name);
    const membersChanged =
      !sameSet(currentTables, selectedTableIds) || !sameSet(currentFns, selectedFunctionNames);
    if (membersChanged) {
      const members = await setDataProductMembers(
        productId,
        selectedTableIds.map(Number),
        selectedFunctionNames,
      );
      if (!members.success) {
        setMsg(members.message || t("dataProductsTab.saveFailed"));
        products.reload();
        return;
      }
      await refetchTables();
      const refreshed = await fetchActions();
      setFunctions(refreshed.functions);
    }
    // REQ-2180: a product's contracts are derived from its offers; none is written here.
    products.reload();
    setCoreVersion((v) => v + 1);
    setMembersVersion((v) => v + 1);
    closeForm();
  };

  // REQ-1634 STATUS: a status change. The products, their findings and the panel's core are read
  // again. Only a retirement moves members (to the successor), so only it reads the tables, the
  // commands, the lineage and the contracts again: the tables are every registered table, the
  // page's heaviest read, and reading them after each change held the badge back.
  const statusChanged = (message: string, membersMoved: boolean) => {
    notifications.show({ color: "green", message });
    products.reload();
    void productFindings.refetch();
    setCoreVersion((v) => v + 1);
    if (!membersMoved) return;
    void refetchTables();
    fetchActions()
      .then((actions) => setFunctions(actions.functions))
      .catch(() => setFunctions([]));
    setMembersVersion((v) => v + 1);
  };

  // REQ-2198: the detail's Delete (PanelDelete) shows the delete's progress and says why one
  // failed; it resolves once the list is read again without the product.
  const handleDelete = async (id: string) => {
    const result = await deleteDataProduct(id);
    if (refusal.refused(result, id)) return false;
    if (!result.success) throw new Error(result.message || t("dataProductsTab.deleteFailed"));
    // The deleted product's panel closes with it, unless another has been opened meanwhile.
    detail.close({ only: id, replace: true });
    await products.reload();
  };

  const formCard = (
    <DataProductFormCard
      editingId={editingId}
      form={form}
      setForm={setForm}
      domainOptions={domainOptions}
      roleOptions={roleOptions}
      ownerRoleOptions={ownerRoleOptions}
      onPeopleError={onPeopleError}
      tables={tables}
      selectedTableIds={selectedTableIds}
      setSelectedTableIds={setSelectedTableIds}
      functions={functions}
      selectedFunctionNames={selectedFunctionNames}
      setSelectedFunctionNames={setSelectedFunctionNames}
      saving={saving}
      msg={msg}
      onSave={handleSave}
      onCancel={closeForm}
      focus={formFocus}
    />
  );

  // REQ-1940: sort and group are the shared list mechanism.
  const filteredProducts = useMemo(
    () => shownBy(dataProducts, productNarrowings),
    [dataProducts, productNarrowings],
  );
  const productColumns = useMemo<ListColumn<DataProduct>[]>(
    () => [
      { key: "id", label: t("dataProductsTab.colId"), sortValue: (p) => p.id },
      {
        key: "domain",
        label: t("dataProductsTab.colDomain"),
        sortValue: (p) => p.domainId,
        groupValue: (p) => p.domainId,
      },
      { key: "name", label: t("dataProductsTab.colName"), sortValue: (p) => p.name },
      {
        key: "owner",
        label: t("dataProductsTab.colOwner"),
        sortValue: (p) => ownerNames(p).join(", "),
        groupValue: (p) => ownerNames(p).join(", "),
      },
      {
        key: "status",
        label: t("dataProductsTab.colStatus"),
        sortValue: (p) => p.status,
        groupValue: (p) => t(`dataProductsTab.status_${p.status}`),
      },
      {
        // REQ-2259: the product's sync status, from the record of every publish.
        key: "sync",
        label: t("dataProductsTab.colSync"),
        sortValue: (p) => SYNC_ORDER[syncState(syncStatus.statuses[p.id] ?? [])],
        groupValue: (p) =>
          t(`dataProductsTab.syncGroup_${syncState(syncStatus.statuses[p.id] ?? [])}`),
      },
      { key: "purpose", label: t("dataProductsTab.colPurpose"), sortValue: (p) => p.purpose },
    ],
    [t, syncStatus.statuses],
  );
  const sortGroup = useListSortGroup(filteredProducts, productColumns, "data-products");
  // REQ-2240: data products selected and deleted together.
  const shownProductIds = useMemo(() => filteredProducts.map((p) => p.id), [filteredProducts]);
  const selection = useListSelection(shownProductIds);
  const deleteDataProducts = useDeleteDataProducts();

  // The products sharing a dataset with the open one, each with the datasets they share.
  const sharingOf = (id: string): SharingProduct[] => {
    const own = memberTables(id);
    return dataProducts
      .filter((other) => other.id !== id)
      .map((other) => ({
        id: other.id,
        name: other.name,
        datasets: own
          .filter((tb) => tb.productIds.includes(other.id))
          .map((tb) => catalogTableName(tb)),
      }))
      .filter((other) => other.datasets.length > 0);
  };

  // The right-hand panel: the form that creates a product while it is open, otherwise the
  // selected product -- its detail, with the form that edits it in place of its header.
  const closePanel = () => {
    closeForm();
    setExpanded(null);
  };
  const selectedProduct =
    expanded === null ? null : (dataProducts.find((p) => p.id === expanded) ?? null);
  const panelTitle = creating
    ? t("dataProductsTab.createTitle")
    : selectedProduct
      ? selectedProduct.name
      : "";
  const panelBody = creating ? (
    <div data-testid="data-product-create-card">{formCard}</div>
  ) : selectedProduct ? (
    <div data-testid="data-product-detail">
      <DataProductDetailPanel
        p={selectedProduct}
        core={core.data}
        coreError={core.error}
        reader={reader}
        viewer={viewer}
        canEdit={mayEdit(selectedProduct)}
        canDelete={mayEdit(selectedProduct)}
        owner={ownsProduct(selectedProduct)}
        seesConsumers={isProductOwner(selectedProduct, userId, selectedRoles)}
        lineageLoading={lineageLoading}
        lineageError={lineageError}
        lineageGraph={lineageGraph}
        relationLabel={relationLabel}
        contractsVersion={membersVersion}
        tables={tables}
        sharing={sharingOf(selectedProduct.id)}
        relationships={relationships}
        actingRole={actingRoles}
        editForm={editingId === selectedProduct.id ? formCard : undefined}
        onTagsChanged={() => setCoreVersion((v) => v + 1)}
        onEdit={(focus) => openEdit(selectedProduct, focus)}
        onDuplicate={canEdit ? () => openDuplicate(selectedProduct) : undefined}
        onDelete={() => handleDelete(selectedProduct.id)}
        products={dataProducts}
        members={[
          ...memberTables(selectedProduct.id).map((tb) => tb.tableName),
          ...memberFunctions(selectedProduct.id).map((fn) => fn.name),
        ]}
        findings={productFindings.findings.get(selectedProduct.id) ?? []}
        onStatusChanged={statusChanged}
        onStatusFailed={setMsg}
        statusActions={statusActions}
        openProduct={(id) => {
          closeForm();
          setExpanded(id);
        }}
      />
    </div>
  ) : null;

  return (
    // The list and its panel share the height the heading leaves; each scrolls by itself.
    <div className="page page-sticky-head" data-tour="data-products-content">
      <div className="page-header">
        <PageTitle icon={Package}>{t("dataProductsTab.title")}</PageTitle>
        <FilterInput
          value={search}
          onChange={updateSearch}
          placeholder={t("dataProductsTab.filterPlaceholder")}
        />
        <PageActions>
          {/* Hidden while its form is open: the form ends with Save and Cancel. */}
          {canEdit && !creating && (
            <PageCommand description={t("pageCommands.addDataProduct")} docs="data-products/">
              <Button
                leftSection={<Plus size={13} />}
                onClick={openCreate}
                data-testid="data-products-new-button"
              >
                {t("dataProductsTab.newDataProduct")}
              </Button>
            </PageCommand>
          )}
          <PageCommand description={t("pageCommands.productMap")} docs="data-products/">
            <ProductMapButton />
          </PageCommand>
        </PageActions>
      </div>
      {/* The list's facets, under the header's filter. */}
      <Group gap="sm" mb="sm" wrap="wrap">
        <div data-testid="data-products-tag-facet" style={{ minWidth: 180, maxWidth: 360 }}>
          <MultiSelect
            options={tagOptions}
            value={tagFilter}
            onChange={setTagFilter}
            ariaLabel={t("dataProductsTab.tagFacetLabel")}
            placeholder={t("dataProductsTab.tagFacetPlaceholder")}
          />
        </div>
        <Checkbox
          size="xs"
          label={t("dataProductsTab.toReview")}
          checked={toReviewOnly}
          onChange={(e) => setToReviewOnly(e.currentTarget.checked)}
          styles={{ label: { whiteSpace: "nowrap" } }}
          data-testid="data-products-to-review-only"
        />
      </Group>

      {error && (
        <Alert color="red" mb="sm">
          {error}
        </Alert>
      )}
      {syncStatus.error && (
        <Alert color="red" mb="sm" data-testid="data-products-sync-error">
          {t("dataProductsTab.syncUnread", { error: syncStatus.error })}
        </Alert>
      )}
      {products.unreadable && (
        <Alert color="yellow" mb="sm" data-testid="data-products-unreadable">
          {t("dataProductCore.unreadable")}
        </Alert>
      )}
      {msg && !creating && editingId === null && (
        <Alert color="red" mb="sm" withCloseButton onClose={() => setMsg("")}>
          {msg}
        </Alert>
      )}

      {(() => {
        if (loading && dataProducts.length === 0) {
          return <PageLoading message={t("dataProductsTab.loading")} />;
        }
        return (
          <SplitPane
            outOfList={detail.outOfList}
            testId="data-products-split"
            storageKey="provisa.data_products.detailWidth"
            title={panelTitle}
            onClose={closePanel}
            // Escape closes a product that is only being looked at; it never drops what is typed.
            closeOnEscape={!creating && editingId === null}
            labels={{
              panel: t("dataProductsTab.detailPanel"),
              resize: t("tablesPage.resizeDetail"),
              expand: t("tablesPage.expandDetail"),
              restore: t("tablesPage.restoreDetail"),
              close: t("tablesPage.closeDetail"),
              backToList: t("tablesPage.backToList"),
            }}
            list={
              <>
                {/* REQ-1327: the bulk bar and the row checkboxes are for a holder of data_product_rw. */}
                {canEdit && (
                  <ListBulkBar
                    selection={selection}
                    nameOf={(id) => id}
                    onDelete={deleteDataProducts}
                    testId="data-products-bulk"
                  />
                )}
                <ListTable testId="data-products-table" selection={canEdit ? selection : undefined}>
                  <ListHead
                    sortGroup={sortGroup}
                    columns={[
                      { col: "id" },
                      { col: "domain" },
                      { col: "name" },
                      { col: "owner" },
                      { col: "status" },
                      { col: "sync" },
                      { col: "purpose" },
                    ]}
                  />
                  <Table.Tbody>
                    {/* Unreadable, the banner above says why the list is empty; it is not that
                        none exist. With products read, an empty list is the filters'. */}
                    {filteredProducts.length === 0 && !products.unreadable && (
                      <ListEmpty colSpan={7} testId="data-products-empty">
                        {dataProducts.length === 0
                          ? t("dataProductsTab.empty")
                          : t("dataProductsTab.noMatch")}
                      </ListEmpty>
                    )}
                    <ListItems
                      state={sortGroup}
                      colSpan={7}
                      rowKey={(p) => p.id}
                      render={(p) => {
                        const isExpanded = expanded === p.id;
                        return (
                          <ListRow
                            key={p.id}
                            testId={`data-products-row-${p.id}`}
                            // The row whose detail the right-hand panel is showing.
                            selected={isExpanded}
                            aria-expanded={isExpanded}
                            onClick={() => {
                              // The row opens its detail in the panel, swaps it, or closes it.
                              closeForm();
                              setExpanded(isExpanded ? null : p.id);
                            }}
                          >
                            <Table.Td>
                              {/* REQ-1659 (amended 2026-11-28): its tags, edited here as on
                                  the other list pages. */}
                              <Group gap={4} wrap="wrap">
                                <Text size="sm" fw={600} ff="monospace" truncate="end">
                                  {p.id}
                                </Text>
                                <TagControl
                                  objectType="product"
                                  productId={p.id}
                                  readOnly={!mayEdit(p)}
                                  onChanged={() => setCoreVersion((v) => v + 1)}
                                />
                              </Group>
                            </Table.Td>
                            <Table.Td>
                              <Text size="xs" truncate="end">
                                {p.domainId}
                              </Text>
                            </Table.Td>
                            <Table.Td>
                              <Text size="xs">{p.name}</Text>
                            </Table.Td>
                            <Table.Td>
                              <Group gap={4} wrap="nowrap">
                                <Text size="xs" truncate="end">
                                  {ownerNames(p).join(", ")}
                                </Text>
                                {p.namedOwners.length > 0 && (
                                  <OwnerResolutionIcon
                                    refs={ownerNames(p)}
                                    ariaLabel={t("dataProductsTab.resolveOwner", { id: p.id })}
                                  />
                                )}
                              </Group>
                            </Table.Td>
                            <Table.Td>
                              <ProductStatusBadge
                                status={p.status}
                                testId={`data-products-status-${p.id}`}
                              />
                            </Table.Td>
                            <Table.Td>
                              <ProductSyncCell
                                productId={p.id}
                                statuses={syncStatus.statuses[p.id] ?? []}
                              />
                            </Table.Td>
                            <Table.Td>
                              <Text size="xs" c="var(--text-muted)" lineClamp={3}>
                                {p.purpose}
                              </Text>
                            </Table.Td>
                          </ListRow>
                        );
                      }}
                    />
                  </Table.Tbody>
                </ListTable>
              </>
            }
          >
            {panelBody}
          </SplitPane>
        );
      })()}

      {refusal.dialog}
    </div>
  );
}
