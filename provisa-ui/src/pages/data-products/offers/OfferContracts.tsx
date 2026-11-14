// Copyright (c) 2026 Kenneth Stott
// Canary: 8c41f2a7-6d95-4e3b-a0c8-17e5b9d4f263
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 CONTRACTS ARE DERIVED FROM THE OFFERS, THE UI SHOWS CONTRACTS PER OFFER: the contracts an
// offer gives, one row per dataset, each headed by its dataset, version, SLA headline, last check
// and a warning where its newest version breaks a reader. A row opens to the offer's terms on the
// dataset -- its columns as the offer's role sees them in the tables page's columns table (REQ-2239),
// the offer's terms of use, its row rules in words -- and to its ODCS document, read when that
// opens (ContractView). Rows start closed and several may be open at once.

import { useEffect, useState, type CSSProperties } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, List, Loader, Stack, Text } from "@mantine/core";
import { Eye, TriangleAlert } from "lucide-react";
import {
  fetchContractVersions,
  fetchProductContract,
  type OdcsContract,
} from "../../../api/dataProductContracts";
import type { Offer, OfferDatasetContract } from "../../../api/dataProductOffers";
import type { ProductLineage } from "../../../api/lineage";
import type { MetaReader } from "../../../api/metaGraph";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";
import { NEVER_EXPIRES } from "../../../components/form/cacheTtl";
import { Tip } from "../../../components/Tip";
import type { ContractVersion } from "../../../types/admin";
import { ColumnsTable } from "../../tables/ColumnsTable";
import { ContractView, type ContractViewer } from "../ContractView";
import { modelName, type CoreTable } from "../productCore";
import { contractColumns, memberOf } from "./offerTerms";
import { TermsOfUse } from "./TermsOfUse";

/** What a contract's heading and its document read besides the offer: the product's datasets as
 *  the meta domain names them, its lineage (the checks on each), and who reads it. */
export interface ContractContext {
  ports: CoreTable[];
  lineage: ProductLineage | null;
  reader: MetaReader | null;
  viewer: ContractViewer;
}

// A dataset name is one value: it keeps to one line, cut with an ellipsis and shown whole on hover.
const PORT_NAME: CSSProperties = { maxWidth: 320, minWidth: 0 };

/** One contract's row heading: its dataset, version, SLA headline, last check and the warning that
 *  applies. */
function RowTitle({
  offer,
  entry,
  port,
  lineage,
}: {
  offer: Offer;
  entry: OfferDatasetContract;
  port: CoreTable | undefined;
  lineage: ProductLineage | null;
}) {
  const { t, i18n } = useTranslation();
  const id = entry.table_id;
  const testId = `offer-${offer.id}-contract-${id}`;
  // The latest run of the product's promises on this dataset (REQ-2132).
  const runs = lineage
    ? lineage.dq.promises
        .filter((c) => c.checked_table_id === id && c.latest_run_at !== null)
        .map((c) => c.latest_run_at as string)
        .sort()
    : null;
  const lastRun = runs && runs.length > 0 ? runs[runs.length - 1] : null;
  // The SLA headline: the dataset's freshness (REQ-2118), entered on the product, never per offer.
  const ttl = port?.cacheTtl;
  const sla =
    ttl !== null && ttl !== undefined
      ? ttl === NEVER_EXPIRES
        ? t("dataProductsTab.contract.loadedOnce")
        : t("dataProductsTab.contract.freshness", { seconds: ttl })
      : t("dataProductsTab.contract.noSla");
  const name = port ? modelName(port) : entry.table;
  // REQ-2180: changing an offer versions its contracts; one that removes or masks a column breaks.
  const versions = entry.contract.versions ?? [];
  const newest = versions[versions.length - 1];
  const breaking = newest?.changes.some((c) => c.breaking) ? newest : undefined;
  return (
    <Group gap={8} wrap="wrap" style={{ minWidth: 0 }} data-testid={`${testId}-row`}>
      <Tip text={name}>
        <Text size="sm" ff="monospace" fw={500} truncate="end" style={PORT_NAME}>
          {name}
        </Text>
      </Tip>
      <Badge size="sm" variant="light" data-testid={`${testId}-version`}>
        {t("dataProductsTab.contract.versionBadge", { version: entry.contract.version })}
      </Badge>
      <Text size="xs" c="dimmed" data-testid={`${testId}-sla`}>
        {sla}
      </Text>
      {runs && (
        <Text size="xs" c="dimmed" data-testid={`${testId}-checked`}>
          {lastRun
            ? t("dataProductsTab.contract.checkedAt", {
                when: new Date(lastRun).toLocaleString(i18n.language),
              })
            : t("dataProductsTab.contract.neverChecked")}
        </Text>
      )}
      {breaking && (
        <Badge
          size="sm"
          variant="light"
          color="red"
          leftSection={<TriangleAlert size={12} aria-hidden />}
          data-testid={`${testId}-breaking`}
        >
          {t("dataProductOffers.breakingIn", { version: breaking.version })}
        </Badge>
      )}
    </Group>
  );
}

/** The contract's ODCS document and its versions, read when it opens. */
function ContractDocument({
  productId,
  offer,
  port,
  context,
}: {
  productId: string;
  offer: Offer;
  port: CoreTable;
  context: ContractContext;
}) {
  const { t } = useTranslation();
  const member = memberOf(port);
  const key = `${productId}\n${offer.name}\n${member}\n${offer.version}`;
  const [read, setRead] = useState<{
    key: string;
    doc?: OdcsContract;
    versions?: ContractVersion[];
    error?: string;
  } | null>(null);
  useEffect(() => {
    let cancelled = false;
    Promise.all([
      fetchProductContract(productId, offer.name, member),
      fetchContractVersions(productId, offer.name, member),
    ])
      .then(([doc, versions]) => {
        if (!cancelled) setRead({ key, doc, versions });
      })
      .catch((e: Error) => {
        if (!cancelled) setRead({ key, error: e.message });
      });
    return () => {
      cancelled = true;
    };
  }, [productId, offer.name, member, key]);
  if (read === null || read.key !== key) {
    return <Loader size="sm" data-testid={`data-contract-loading-${port.id}`} />;
  }
  if (read.error !== undefined) {
    return (
      <Alert color="red" data-testid={`data-contract-error-${port.id}`}>
        {t("dataProductsTab.contract.loadError", { error: read.error })}
      </Alert>
    );
  }
  const { lineage, reader, viewer } = context;
  return (
    <ContractView
      contract={read.doc as OdcsContract}
      versions={read.versions as ContractVersion[]}
      tableId={port.id}
      promises={lineage ? lineage.dq.promises.filter((c) => c.checked_table_id === port.id) : null}
      table={port}
      reader={reader}
      viewer={viewer}
    />
  );
}

/** The offer's terms on one dataset, and its document. */
function DatasetContract({
  productId,
  offer,
  entry,
  port,
  context,
  onPreview,
}: {
  productId: string;
  offer: Offer;
  entry: OfferDatasetContract;
  port: CoreTable | undefined;
  context: ContractContext | null;
  onPreview: (tableId: number) => void;
}) {
  const { t } = useTranslation();
  const terms = entry.contract.terms;
  const testId = `offer-${offer.id}-contract-${entry.table_id}`;
  return (
    <Stack gap="xs" data-testid={testId}>
      <Group gap="xs" justify="space-between" wrap="wrap">
        {terms.sample_views.length > 0 ? (
          <Text size="xs" c="dimmed">
            {t("dataProductOffers.sampleViews", { views: terms.sample_views.join(", ") })}
          </Text>
        ) : (
          <span />
        )}
        {terms.data_level !== "synthetic" && (
          <Button
            size="compact-xs"
            variant="subtle"
            leftSection={<Eye size={12} />}
            onClick={() => onPreview(entry.table_id)}
            data-testid={`${testId}-preview`}
          >
            {t("dataProductOffers.preview")}
          </Button>
        )}
      </Group>
      <ColumnsTable
        readOnly
        columns={contractColumns(terms, offer.role_id)}
        roles={[]}
        irTypes={[]}
        sourceType=""
        tableId={entry.table_id}
        testId={`${testId}-columns`}
      />
      <Text size="xs" fw={600}>
        {t("dataProductOffers.termsOfUse")}
      </Text>
      <TermsOfUse terms={terms} testId={`${testId}-terms`} />
      <Text size="xs" fw={600}>
        {t("dataProductOffers.rowRules")}
      </Text>
      {terms.row_rules.length === 0 ? (
        <Text size="xs" c="dimmed" data-testid={`${testId}-no-row-rules`}>
          {t("dataProductOffers.noRowRules")}
        </Text>
      ) : (
        <List size="xs" data-testid={`${testId}-row-rules`}>
          {terms.row_rules.map((rule) => (
            <List.Item key={rule}>{rule}</List.Item>
          ))}
        </List>
      )}
      {terms.profiles.length > 0 && (
        <Text size="xs" c="dimmed" data-testid={`${testId}-profiles`}>
          {t("dataProductOffers.profileTables", { tables: terms.profiles.join(", ") })}
        </Text>
      )}
      {context && port && (
        <Collapsibles id={`data-product.${productId}.offer.${offer.id}.document.${entry.table_id}`}>
          <CollapsibleItem
            value="document"
            defaultOpen={false}
            mountWhenOpen
            controlTestId={`${testId}-document-toggle`}
            title={
              <Text size="sm" fw={500}>
                {t("dataProductOffers.contractDocument")}
              </Text>
            }
          >
            <ContractDocument productId={productId} offer={offer} port={port} context={context} />
          </CollapsibleItem>
        </Collapsibles>
      )}
    </Stack>
  );
}

export function OfferContracts({
  productId,
  offer,
  context,
  onPreview,
}: {
  productId: string;
  offer: Offer;
  /** Null where the product's datasets are not read (the contracts' documents are then not shown). */
  context: ContractContext | null;
  onPreview: (tableId: number) => void;
}) {
  const { t } = useTranslation();
  const ports = context?.ports ?? [];
  if (offer.contracts.length === 0) {
    return (
      <Text size="sm" c="dimmed">
        {t("dataProductOffers.noContracts")}
      </Text>
    );
  }
  return (
    <Collapsibles id={`data-product.offer.${productId}.${offer.id}.datasets`}>
      {offer.contracts.map((entry) => {
        const port = ports.find((p) => p.id === entry.table_id);
        return (
          <CollapsibleItem
            key={entry.table_id}
            value={String(entry.table_id)}
            defaultOpen={false}
            mountWhenOpen
            controlTestId={`offer-${offer.id}-contract-${entry.table_id}-toggle`}
            title={
              <RowTitle
                offer={offer}
                entry={entry}
                port={port}
                lineage={context?.lineage ?? null}
              />
            }
          >
            <DatasetContract
              productId={productId}
              offer={offer}
              entry={entry}
              port={port}
              context={context}
              onPreview={onPreview}
            />
          </CollapsibleItem>
        );
      })}
    </Collapsibles>
  );
}
