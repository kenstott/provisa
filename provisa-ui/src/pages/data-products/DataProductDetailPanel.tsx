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
// absent. REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS): a short header, then one
// section per subject in a fixed order -- Offers, Quality, Profile, Lineage, Related, Requests (its
// owner's alone), Access -- each a row of the application's collapsible whose one-line summary is
// computed from its content, expanded in place, open or closed as this viewer left it. Offers is
// open at first; a graph is drawn only inside an expanded row. REQ-2160: one page for owner and
// reader alike; what a reader may do is their rights', not another page's. The same rows for the
// read view and the edit view, each composed of the components the tables page uses.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Alert, Group, Loader, Stack, Text } from "@mantine/core";
import { Pencil } from "lucide-react";
import type { DataProduct, ListedTable, Relationship } from "../../types/admin";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import { Tip } from "../../components/Tip";
import { PanelDelete } from "../../components/list/PanelDelete";
import { PanelDuplicate } from "../../components/list/PanelDuplicate";
import { PanelActions } from "../../components/list/SplitPane";
import type { DetailField } from "../../components/detail/DetailFields";
import type { ProductLineage } from "../../api/lineage";
import type { MetaReader } from "../../api/metaGraph";
import { tableSqlRef } from "../../naming";
import { useProductLifecycle } from "./ProductLifecycle";
import type { DataProductStatus, ProductFinding } from "../../hooks/useDataProductMutations";
import { modelName, type ProductCore } from "./productCore";
import { ProductHeader } from "./ProductHeader";
import { AccessSection } from "./AccessSection";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";
import type { Bid, Offer, ProductProfile } from "../../api/dataProductOffers";
import { qualitySummary, useQualityWords } from "./productQuality";
import { WorstBadge } from "./QualityStatus";
import type { ContractViewer } from "./ContractView";
import { OffersTab } from "./offers/OffersTab";
import type { OfferDataset } from "./offers/AddOfferForm";
import { QualityTab } from "./tabs/QualityTab";
import { ProfileTab } from "./tabs/ProfileTab";
import { LineageTab } from "./tabs/LineageTab";
import { RelatedTab, type SharingProduct } from "./tabs/RelatedTab";
import { RequestsTab } from "./tabs/RequestsTab";
import { PRODUCT_SECTIONS, type ProductSection } from "./productSections";
import { TablePreviewPane } from "../tables/TablePreviewPane";

interface DataProductDetailPanelProps {
  p: DataProduct;
  /** The meta-domain core of the panel; null while it is read. */
  core: ProductCore | null;
  coreError: string | null;
  /** The acting role's schema and root fields, for the member tables' drill-down queries. */
  reader: MetaReader | null;
  /** Who reads the panel: what a contract's How to use offers them (REQ-2158). */
  viewer: ContractViewer;
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
  // Bumped when the product's members can have changed (a save, a retirement): its offers and
  // their contracts are read again.
  contractsVersion?: number;
  /** Every registered table the reader lists: a dataset's (or sample view's) rows are previewed
   *  through it. */
  tables?: ListedTable[];
  /** The products sharing one of this product's datasets. */
  sharing?: SharingProduct[];
  /** The model's relationships, for each dataset's associations. */
  relationships?: Relationship[];
  /** The role (or roles) the reader acts as: a preview's rows and the profile are read as it. */
  actingRole?: string | null;
  /** The edit form, in place of the header's values while the product is edited; the rows stay. */
  editForm?: ReactNode;
  /** REQ-1634: a tag was put on the product or taken off; its core is read again. */
  onTagsChanged: () => void;
  /** Opens the edit form, on ``focus`` where given. */
  onEdit: (focus?: "supportContact") => void;
  /** Opens the create form filled from the product; given to a caller who may create products. */
  onDuplicate?: () => void;
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

/** A section keeps its content inside it: what cannot wrap scrolls within the section. */
const SECTION_BODY = { overflowX: "auto", overflowWrap: "anywhere" } as const;

/** The sections whose content is a graph, or holds one: mounted only while expanded. */
const DRAWS_A_GRAPH: ReadonlySet<ProductSection> = new Set(["lineage", "related"]);

export function DataProductDetailPanel({
  p,
  core,
  coreError,
  reader,
  viewer,
  canEdit,
  canDelete,
  owner,
  seesConsumers,
  lineageLoading,
  lineageError,
  lineageGraph,
  relationLabel,
  contractsVersion = 0,
  tables = [],
  sharing = [],
  relationships = [],
  actingRole = null,
  editForm,
  onTagsChanged,
  onEdit,
  onDuplicate,
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
  // A section a link inside the page asks to expand (See Quality, an input's lineage).
  const [reveal, setReveal] = useState<ProductSection | null>(null);
  useEffect(() => {
    // Expanded and remembered so by the collapsible, which opens an item when ``reveal`` changes:
    // cleared once consumed, the same link asked again expands it again.
    // eslint-disable-next-line react-hooks/set-state-in-effect -- a one-shot request, consumed.
    if (reveal) setReveal(null);
  }, [reveal]);
  // What each section's summary is computed from, as its content reads it.
  const [offers, setOffers] = useState<Offer[] | null>(null);
  const [profiles, setProfiles] = useState<ProductProfile[] | null>(null);
  const [requests, setRequests] = useState<Bid[] | null>(null);
  const qualityWords = useQualityWords();
  // A dataset whose rows are looked at, in place of the rows.
  const [preview, setPreview] = useState<ListedTable | null>(null);
  // Bumped when a bid is made: the Requests section reads them again.
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
  const shown = PRODUCT_SECTIONS.filter((s) => s !== "requests" || owner);
  const promises = lineageGraph?.dq.promises;
  const quality = promises ? qualitySummary(promises) : null;

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

  const actions = (canEdit || canDelete || onDuplicate) && !editForm && (
    <PanelActions>
      {lifecycle.actions}
      {canEdit && (
        <Tip text={t("dataProductsTab.editDataProduct", { id: p.id })} named>
          <ActionIcon
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
      {onDuplicate && (
        <PanelDuplicate
          name={p.id}
          onDuplicate={onDuplicate}
          testId={`data-product-detail-duplicate-${p.id}`}
        />
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

  /** A section's one-line summary, computed from its content; an ellipsis while it is read. */
  const summary = (section: ProductSection): string => {
    const pending = "…";
    switch (section) {
      case "offers": {
        if (offers === null) return pending;
        const open = offers.filter((o) => o.open).length;
        const review = offers.filter((o) => o.review !== null).length;
        return [
          t("dataProductOffers.summaryOffers", { count: offers.length }),
          open > 0 ? t("dataProductOffers.summaryOpen", { count: open }) : null,
          review > 0 ? t("dataProductOffers.summaryInReview", { count: review }) : null,
        ]
          .filter(Boolean)
          .join(" · ");
      }
      case "quality":
        return quality ? qualityWords(quality) : pending;
      case "profile": {
        if (!actingRole) return t("dataProductOffers.summaryProfileNoRole");
        if (profiles === null) return pending;
        return t("dataProductOffers.summaryProfiles", {
          profiled: profiles.filter((pr) => pr.run !== null).length,
          count: datasets.length,
        });
      }
      case "lineage":
        return lineageGraph
          ? [
              t("dataProductOffers.summaryDatasets", { count: datasets.length }),
              t("dataProductOffers.summaryInputs", { count: lineageGraph.inputs.length }),
            ].join(" · ")
          : pending;
      case "related":
        return [
          t("dataProductOffers.summarySharing", { count: sharing.length }),
          t("dataProductOffers.summaryRelatedTables", {
            count: core.edges?.filter((e) => e.kind === "related").length ?? 0,
          }),
          t("dataProductOffers.summaryTerms", { count: core.terms?.length ?? 0 }),
        ].join(" · ");
      case "requests": {
        if (requests === null) return pending;
        const waiting = requests.filter((b) => b.status === "pending").length;
        return [
          t("dataProductOffers.summaryRequests", { count: requests.length }),
          waiting > 0 ? t("dataProductOffers.summaryPending", { count: waiting }) : null,
        ]
          .filter(Boolean)
          .join(" · ");
      }
      case "access":
        return t("dataProductOffers.summaryAccess", { count: datasets.length });
    }
  };

  /** A section's content, expanded in place. */
  const body = (section: ProductSection): ReactNode => {
    switch (section) {
      case "offers":
        return (
          <OffersTab
            productId={p.id}
            owner={owner}
            canEdit={canEdit}
            datasets={datasets}
            views={views}
            onShowQuality={() => setReveal("quality")}
            onPreview={previewTable}
            onQueryNow={queryNow}
            onBid={() => setBids((n) => n + 1)}
            context={{ ports: outputTables ?? [], lineage: lineageGraph, reader, viewer }}
            version={contractsVersion}
            onLoaded={setOffers}
          />
        );
      case "quality":
        return (
          <QualityTab
            productId={p.id}
            owner={owner}
            dq={lineageGraph?.dq}
            lineageGraph={lineageGraph}
            lineageLoading={lineageLoading}
            lineageError={lineageError}
            outputTables={outputTables}
            dqTables={dqTables}
            offers={offers}
            onShowLineage={() => setReveal("lineage")}
          />
        );
      case "profile":
        return (
          <ProfileTab productId={p.id} owner={owner} role={actingRole} onLoaded={setProfiles} />
        );
      case "lineage":
        return (
          <LineageTab
            productId={p.id}
            loading={lineageLoading}
            error={lineageError}
            graph={lineageGraph}
            relationLabel={relationLabel}
          />
        );
      case "related":
        return (
          <RelatedTab
            productId={p.id}
            core={core}
            seesConsumers={seesConsumers}
            lineageGraph={lineageGraph}
            lineageLoading={lineageLoading}
            lineageError={lineageError}
            sharing={sharing}
            relationships={relationships}
            openProduct={openProduct}
          />
        );
      case "requests":
        return (
          <RequestsTab key={bids} productId={p.id} datasets={datasets} onLoaded={setRequests} />
        );
      case "access":
        return <AccessSection core={core} reader={reader} />;
    }
  };

  if (preview) {
    return (
      <div className="split-pane__fill" data-testid={`data-product-preview-${p.id}`}>
        {actions}
        <TablePreviewPane table={preview} role={actingRole} onBack={() => setPreview(null)} />
      </div>
    );
  }

  return (
    <Stack gap="sm" data-testid={`data-product-detail-${p.id}`}>
      {actions}
      {lifecycle.toReview}
      {editForm ?? (
        <ProductHeader
          core={core}
          fields={fields}
          quality={quality}
          consumers={seesConsumers && lineageGraph ? lineageGraph.consumers.length : null}
          canEdit={canEdit}
          onTagsChanged={onTagsChanged}
        />
      )}
      <Collapsibles
        id="data-product.sections"
        reveal={reveal}
        testId={`data-product-sections-${p.id}`}
      >
        {shown.map((section) => (
          <CollapsibleItem
            key={section}
            value={section}
            defaultOpen={section === "offers"}
            mountWhenOpen={DRAWS_A_GRAPH.has(section)}
            testId={`data-product-row-${section}-${p.id}`}
            controlTestId={`data-product-row-${section}-${p.id}-toggle`}
            title={
              <Group
                gap="sm"
                wrap="nowrap"
                style={{ minWidth: 0 }}
                // The guided tour's anchor for a product's data quality: its row, always there.
                data-tour={section === "quality" ? "data-product-dq" : undefined}
              >
                <Text
                  size="sm"
                  fw={600}
                  style={{ flexShrink: 0 }}
                  data-testid={`data-product-row-${section}-${p.id}-name`}
                >
                  {t(`dataProductOffers.section_${section}`)}
                </Text>
                {section === "quality" && quality?.worst && <WorstBadge worst={quality.worst} />}
                <Text
                  size="xs"
                  c="dimmed"
                  truncate="end"
                  data-testid={`data-product-row-${section}-${p.id}-summary`}
                >
                  {summary(section)}
                </Text>
              </Group>
            }
          >
            <div style={SECTION_BODY}>{body(section)}</div>
          </CollapsibleItem>
        ))}
      </Collapsibles>
      {lifecycle.dialogs}
    </Stack>
  );
}
