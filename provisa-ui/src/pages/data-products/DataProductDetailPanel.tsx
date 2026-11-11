// Copyright (c) 2026 Kenneth Stott
// Canary: 0f62c0c5-a9e2-4f0c-812f-d84dd160d2c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2127: the Data Product detail panel, the right-hand panel of the products list. Its core --
// the product's fields, member tables, commands, tags, glossary terms, related tables -- is read
// from the meta domain as the signed-in user (productCore.ts); a part the role cannot read there is
// absent. REQ-2180 OFFERS, REQ-2239 (amended 2026-11-11): a header (owner, domain, freshness, a
// query to run now, every value of the product), then tabs built around the product's offers --
// Offers, Quality, Profile, Lineage, Related and, for its owner, Requests -- the same tabs for the
// read view and the edit view. Each tab is composed of the components the tables page uses.

import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Alert, Loader, Stack, Tabs, Text } from "@mantine/core";
import { Pencil } from "lucide-react";
import type { DataProduct, ListedTable, Relationship } from "../../types/admin";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import { Tip } from "../../components/Tip";
import { PanelDelete } from "../../components/list/PanelDelete";
import { PanelActions } from "../../components/list/SplitPane";
import type { DetailField } from "../../components/detail/DetailFields";
import type { ProductLineage } from "../../api/lineage";
import type { MetaReader } from "../../api/metaGraph";
import { tableSqlRef } from "../../naming";
import { useProductLifecycle } from "./ProductLifecycle";
import type { DataProductStatus, ProductFinding } from "../../hooks/useDataProductMutations";
import { modelName, type ProductCore } from "./productCore";
import { ProductHeader } from "./ProductHeader";
import { OffersTab } from "./offers/OffersTab";
import type { OfferDataset } from "./offers/AddOfferForm";
import { QualityTab } from "./tabs/QualityTab";
import { ProfileTab } from "./tabs/ProfileTab";
import { LineageTab } from "./tabs/LineageTab";
import { RelatedTab, type SharingProduct } from "./tabs/RelatedTab";
import { RequestsTab } from "./tabs/RequestsTab";
import { PRODUCT_TABS, type ProductTab } from "./tabs/productTabs";
import { TablePreviewPane } from "../tables/TablePreviewPane";
import { useAuth } from "../../context/AuthContext";


interface DataProductDetailPanelProps {
  p: DataProduct;
  /** The meta-domain core of the panel; null while it is read. */
  core: ProductCore | null;
  coreError: string | null;
  /** The acting role's schema and root fields, for the member tables' drill-down queries. */
  reader: MetaReader | null;
  // REQ-1634, REQ-2188: gates Edit and Delete: data_product_rw, on the products its rules leave it.
  canEdit: boolean;
  canDelete: boolean;
  /** REQ-2160: the reader holds the product's owner or team role: they see its expectations,
   *  related and unattached checks, its full profile and its requests; they add offers. Any other
   *  reader sees its promises, a read-only profile, and bids. The split never grants a right. */
  owner: boolean;
  /** REQ-2174 (amended 2026-11-07), REQ-2219: the reader owns the product (productOwner.ts): only
   *  then are its consumers shown. */
  seesConsumers: boolean;
  lineageLoading: boolean;
  lineageError: string;
  // REQ-1640, REQ-2156: the product's tables, their upstream ancestry and one hop downstream, with
  // its nearest physical inputs, cut to what the reader may read.
  lineageGraph: ProductLineage | null;
  // A lineage relation as the catalog names its table.
  relationLabel?: (relation: string) => string;
  /** Every registered table the reader lists: a dataset's (or sample view's) rows are previewed
   *  through it. */
  tables?: ListedTable[];
  /** The products sharing one of this product's datasets. */
  sharing?: SharingProduct[];
  /** The model's relationships, for each dataset's associations. */
  relationships?: Relationship[];
  /** The edit form, in place of the header's values while the product is edited; the tabs stay. */
  editForm?: ReactNode;
  /** REQ-1634: a tag was put on the product or taken off; its core is read again. */
  onTagsChanged: () => void;
  /** Opens the edit form, on ``focus`` where given. */
  onEdit: (focus?: "supportContact") => void;
  /** REQ-2198: resolves once the product is deleted and the list read again (PanelDelete). */
  onDelete: () => Promise<boolean | void>;
  // REQ-1634 STATUS, REQ-2146: its lifecycle -- the products a successor is chosen from, the
  // members a retirement moves, its findings, and what follows a status change.
  products: DataProduct[];
  members: string[];
  findings: ProductFinding[];
  /** ``membersMoved``: the change moved the product's members (a retirement). */
  onStatusChanged: (message: string, membersMoved: boolean) => void;
  onStatusFailed: (message: string) => void;
  openProduct: (id: string) => void;
  statusActions: DataProductStatus;
}

export function DataProductDetailPanel({
  p,
  core,
  coreError,
  reader,
  canEdit,
  canDelete,
  owner,
  seesConsumers,
  lineageLoading,
  lineageError,
  lineageGraph,
  relationLabel,
  tables = [],
  sharing = [],
  relationships = [],
  editForm,
  onTagsChanged,
  onEdit,
  onDelete,
  products,
  members,
  findings,
  onStatusChanged,
  onStatusFailed,
  openProduct,
  statusActions,
}: DataProductDetailPanelProps) {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const { actingRoles } = useAuth();
  const [tab, setTab] = useState<ProductTab>("offers");
  // A dataset whose rows are looked at, in place of the tabs.
  const [preview, setPreview] = useState<ListedTable | null>(null);
  // Bumped when a bid is made: the Requests tab reads them again.
  const [bids, setBids] = useState(0);
  // Its lifecycle reads the list's row, so it is drawn while the panel's core is read.
  const lifecycle = useProductLifecycle({
    product: p,
    products,
    members,
    findings,
    canEdit,
    onChanged: onStatusChanged,
    onFailed: onStatusFailed,
    onEdit,
    openProduct,
    status: statusActions,
  });

  if (coreError) {
    return (
      <Alert color="red" data-testid={`data-product-detail-error-${p.id}`}>
        {t("dataProductCore.error", { error: coreError })}
      </Alert>
    );
  }
  if (core === null) {
    return <Loader size="sm" data-testid={`data-product-detail-loading-${p.id}`} />;
  }

  const product = core.product;
  // REQ-1443 clause 10: a checker table (one carrying a contract) is a member only because the
  // table it scans is: its rows are the product's quality evidence, not one of its datasets.
  const outputTables = core.tables?.filter((tb) => tb.dqContract == null);
  const dqTables = core.tables?.filter((tb) => tb.dqContract != null);
  const datasets: OfferDataset[] = (outputTables ?? []).map((tb) => ({
    id: tb.id,
    name: modelName(tb),
  }));
  // The views over the product's datasets a sample offer may name: its readers one hop downstream.
  const views: OfferDataset[] = (lineageGraph?.consumers ?? [])
    .filter((c) => c.kind !== "table")
    .map((c) => ({ id: c.table_id, name: c.alias ? `${c.domain_id}.${c.alias}` : c.table_name }));
  const shownTabs = PRODUCT_TABS.filter((tb) => tb !== "requests" || owner);
  const current = shownTabs.includes(tab) ? tab : "offers";

  // Every value of the product, under the labels its form gives them.
  const fields: DetailField[] = [
    { label: t("dataProductsTab.idLabel"), value: product.id },
    { label: t("dataProductsTab.nameLabel"), value: product.name },
    { label: t("dataProductsTab.domainLabel"), value: product.domainId },
    { label: t("dataProductsTab.versionLabel"), value: product.version },
    ...lifecycle.fields,
    {
      label: t("dataProductsTab.ownerLabel"),
      value: product.ownerRole ? (
        <Stack gap={0}>
          <Text size="sm">{product.ownerRole}</Text>
          <OwnerResolutionInline refs={[product.ownerRole]} />
        </Stack>
      ) : null,
    },
    {
      label: t("dataProductsTab.teamLabel"),
      value: product.teamRole ? (
        <Stack gap={0}>
          <Text size="sm">{product.teamRole}</Text>
          <OwnerResolutionInline refs={[product.teamRole]} />
        </Stack>
      ) : null,
    },
    { label: t("dataProductsTab.supportLabel"), value: product.support },
    { label: t("dataProductsTab.supportContactLabel"), value: product.supportContact },
    { label: t("dataProductsTab.purposeLabel"), value: product.purpose },
    { label: t("dataProductsTab.limitationsLabel"), value: product.limitations },
    { label: t("dataProductsTab.usageLabel"), value: product.usage },
    // REQ-2118: when the product was created; read-only, set when it was first saved.
    {
      label: t("dataProductsTab.createdAtLabel"),
      value: new Date(product.createdAt).toLocaleString(i18n.language),
    },
  ];

  const previewTable = (tableId: number) => {
    const found = tables.find((tb) => tb.id === tableId);
    if (found) setPreview(found);
  };
  const queryNow = () => {
    const first = outputTables?.[0];
    if (!first) return;
    navigate("/sql", {
      state: { sql: `select * from ${tableSqlRef(first)} limit 10`, autoRun: true },
    });
  };

  const actions = (canEdit || canDelete) && !editForm && (
    <PanelActions>
      {lifecycle.actions}
      {canEdit && (
        <Tip text={t("dataProductsTab.editDataProduct", { id: p.id })} named>
          <ActionIcon
            variant="subtle"
            aria-label={t("dataProductsTab.editDataProduct", { id: p.id })}
            data-testid={`data-product-detail-edit-${p.id}`}
            onClick={(e) => {
              e.stopPropagation();
              onEdit();
            }}
          >
            <Pencil size={14} />
          </ActionIcon>
        </Tip>
      )}
      {canDelete && (
        <PanelDelete
          itemKey={p.id}
          title={t("dataProductsTab.deleteTitle")}
          consequence={t("dataProductsTab.deleteConfirm", { id: p.id })}
          ariaLabel={t("dataProductsTab.deleteDataProduct", { id: p.id })}
          testId={`data-product-detail-delete-${p.id}`}
          confirmTestId="data-product-delete-modal"
          onDelete={onDelete}
        />
      )}
    </PanelActions>
  );

  if (preview) {
    return (
      <div className="split-pane__fill" data-testid={`data-product-preview-${p.id}`}>
        {actions}
        <TablePreviewPane table={preview} role={actingRoles} onBack={() => setPreview(null)} />
      </div>
    );
  }

  return (
    <Stack gap="sm" data-testid={`data-product-detail-${p.id}`}>
      {actions}
      {lifecycle.toReview}
      {editForm ?? <ProductHeader core={core} reader={reader} fields={fields} />}
      <Tabs value={current} onChange={(v) => v && setTab(v as ProductTab)} keepMounted={false}>
        <Tabs.List aria-label={t("dataProductOffers.tabsLabel")}>
          {shownTabs.map((tb) => (
            <Tabs.Tab key={tb} value={tb} data-testid={`data-product-tab-${tb}-${p.id}`}>
              {t(`dataProductOffers.tab_${tb}`)}
            </Tabs.Tab>
          ))}
        </Tabs.List>
        <Tabs.Panel value="offers" pt="sm">
          <OffersTab
            productId={p.id}
            owner={owner}
            canEdit={canEdit}
            datasets={datasets}
            views={views}
            onShowQuality={() => setTab("quality")}
            onPreview={previewTable}
            onQueryNow={queryNow}
            onBid={() => setBids((n) => n + 1)}
          />
        </Tabs.Panel>
        <Tabs.Panel value="quality" pt="sm">
          <QualityTab
            productId={p.id}
            owner={owner}
            dq={lineageGraph?.dq}
            lineageGraph={lineageGraph}
            outputTables={outputTables}
            dqTables={dqTables}
          />
        </Tabs.Panel>
        <Tabs.Panel value="profile" pt="sm">
          <ProfileTab productId={p.id} owner={owner} />
        </Tabs.Panel>
        <Tabs.Panel value="lineage" pt="sm">
          <LineageTab
            productId={p.id}
            loading={lineageLoading}
            error={lineageError}
            graph={lineageGraph}
            relationLabel={relationLabel}
          />
        </Tabs.Panel>
        <Tabs.Panel value="related" pt="sm">
          <RelatedTab
            productId={p.id}
            core={core}
            canEdit={canEdit}
            seesConsumers={seesConsumers}
            lineageGraph={lineageGraph}
            sharing={sharing}
            relationships={relationships}
            onTagsChanged={onTagsChanged}
            openProduct={openProduct}
          />
        </Tabs.Panel>
        {owner && (
          <Tabs.Panel value="requests" pt="sm">
            <RequestsTab key={bids} productId={p.id} datasets={datasets} />
          </Tabs.Panel>
        )}
      </Tabs>
      {lifecycle.dialogs}
    </Stack>
  );
}
