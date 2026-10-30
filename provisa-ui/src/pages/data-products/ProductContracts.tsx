// Copyright (c) 2026 Kenneth Stott
// Canary: 8c41f2a7-6d95-4e3b-a0c8-17e5b9d4f263
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158 (amended 2026-10-29, A PRODUCT'S CONTRACTS ARE A LIST BENEATH IT): an open product's
// contracts as a secondary list beneath its lineage, one row per output port's contract (a command
// port has none): its port, version, status, consumers, SLA headline, when it was last checked and a
// warning where one applies. Rows start closed and several may be open at once; a row reads its
// contract when it opens and shows it in the product's card panels (ContractView). A link naming a
// contract (``?contract=``, the port's address, contract id or table name) opens its row.

import { useEffect, useState, type CSSProperties } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import { Alert, Badge, Group, Loader, Text } from "@mantine/core";
import { TriangleAlert } from "lucide-react";
import {
  fetchContractVersions,
  fetchProductContract,
  type ContractVersion,
  type OdcsContract,
} from "../../api/dataProductContracts";
import type { ProductLineage } from "../../api/lineage";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";
import { Tip } from "../../components/Tip";
import { NEVER_EXPIRES } from "../../components/form/cacheTtl";
import { relationName } from "../../naming";
import type { DataContract } from "../../types/admin";
import {
  breakingNotInForce,
  coreContracts,
  modelName,
  type CoreTable,
  type ProductCore,
} from "./productCore";
import { ContractView } from "./ContractView";
import { Empty } from "./DetailCard";

/** A port's address, as the contracts endpoint names it: source.schema.table. */
const memberOf = (c: DataContract) => `${c.sourceId}.${c.schemaName}.${c.tableName}`;

/** The contract a link names, by its port's address, its contract id or its table name. */
function linkedContract(contracts: DataContract[], linked: string | null): string | null {
  if (!linked) return null;
  const found = contracts.find(
    (c) => memberOf(c) === linked || c.contractId === linked || c.tableName === linked,
  );
  return found ? memberOf(found) : null;
}

// A port name is one value: it keeps to one line, cut with an ellipsis and shown whole on hover.
const PORT_NAME: CSSProperties = { maxWidth: 320, minWidth: 0 };

/** One contract's row heading: its port, version, status, consumers, SLA headline, last check and
 * the warning that applies. */
function RowTitle({
  contract,
  port,
  lineage,
  owner,
}: {
  contract: DataContract;
  port: CoreTable;
  lineage: ProductLineage | null;
  owner: boolean;
}) {
  const { t, i18n } = useTranslation();
  const id = contract.tableId;
  const relation = relationName({ domainId: port.domain_id, tableName: port.table_name });
  // REQ-2160: who reads the port, as the owner is shown who reads the product.
  const consumers =
    owner && lineage ? lineage.consumers.filter((c) => c.reads.includes(relation)) : null;
  // The latest run of the product's promises on this port (REQ-2132).
  const runs = lineage
    ? lineage.dq.promises
        .filter((c) => c.checked_table_id === id && c.latest_run_at !== null)
        .map((c) => c.latest_run_at as string)
        .sort()
    : null;
  const lastRun = runs && runs.length > 0 ? runs[runs.length - 1] : null;
  // The SLA headline: the port's freshness first, as the contract lists it (REQ-2118), else the
  // first service level it declares.
  const ttl = port.cache_ttl;
  const declared = contract.sla[0];
  const sla =
    ttl !== null && ttl !== undefined
      ? ttl === NEVER_EXPIRES
        ? t("dataProductsTab.contract.loadedOnce")
        : t("dataProductsTab.contract.freshness", { seconds: ttl })
      : declared
        ? [declared.property, `${declared.value ?? ""}${declared.unit ?? ""}`]
            .filter(Boolean)
            .join(" ")
        : t("dataProductsTab.contract.noSla");
  const name = modelName(port);
  // REQ-2158: the newest version not yet in force that breaks a consumer, which the warning names.
  const breaking = breakingNotInForce(contract.versions ?? [])[0];
  return (
    <Group gap={8} wrap="wrap" style={{ minWidth: 0 }} data-testid={`data-contract-row-${id}`}>
      <Tip text={name}>
        <Text size="sm" ff="monospace" fw={500} truncate="end" style={PORT_NAME}>
          {name}
        </Text>
      </Tip>
      <Badge size="sm" variant="light" data-testid={`data-contract-version-${id}`}>
        {t("dataProductsTab.contract.versionBadge", { version: contract.version })}
      </Badge>
      <Badge
        size="sm"
        variant="outline"
        color={contract.status === "active" ? "green" : "gray"}
        data-testid={`data-contract-status-${id}`}
      >
        {t(`dataProductsTab.contract.status.${contract.status}`)}
      </Badge>
      {consumers && (
        <Text size="xs" c="dimmed" data-testid={`data-contract-consumer-count-${id}`}>
          {t("dataProductsTab.contract.consumers", { count: consumers.length })}
        </Text>
      )}
      <Text size="xs" c="dimmed" data-testid={`data-contract-sla-headline-${id}`}>
        {sla}
      </Text>
      {runs && (
        <Text size="xs" c="dimmed" data-testid={`data-contract-checked-${id}`}>
          {lastRun
            ? t("dataProductsTab.contract.checkedAt", {
                when: new Date(lastRun).toLocaleString(i18n.language),
              })
            : t("dataProductsTab.contract.neverChecked")}
        </Text>
      )}
      {contract.status === "draft" && (
        <Badge
          size="sm"
          variant="light"
          color="yellow"
          leftSection={<TriangleAlert size={12} aria-hidden />}
          data-testid={`data-contract-warning-${id}`}
        >
          {t("dataProductsTab.contract.draftWarning")}
        </Badge>
      )}
      {breaking && (
        <Badge
          size="sm"
          variant="light"
          color="red"
          leftSection={<TriangleAlert size={12} aria-hidden />}
          data-testid={`data-contract-breaking-${id}`}
        >
          {t("dataProductsTab.contract.breakingWarning", { version: breaking.contract.version })}
        </Badge>
      )}
    </Group>
  );
}

/** An open row's contract, read when the row opens and again when ``version`` changes. */
function OpenContract({
  productId,
  member,
  version,
  tableId,
  port,
  lineage,
  owner,
}: {
  productId: string;
  member: string;
  version: number;
  tableId: number;
  port: CoreTable;
  lineage: ProductLineage | null;
  owner: boolean;
}) {
  const { t } = useTranslation();
  const [read, setRead] = useState<{
    key: string;
    doc?: OdcsContract;
    versions?: ContractVersion[];
    error?: string;
  } | null>(null);
  const key = `${productId}\n${member}\n${version}`;
  useEffect(() => {
    let cancelled = false;
    Promise.all([fetchProductContract(productId, member), fetchContractVersions(productId, member)])
      .then(([doc, versions]) => {
        if (!cancelled) setRead({ key, doc, versions });
      })
      .catch((e: Error) => {
        if (!cancelled) setRead({ key, error: e.message });
      });
    return () => {
      cancelled = true;
    };
  }, [productId, member, key]);
  if (read === null || read.key !== key) {
    return <Loader size="sm" data-testid={`data-contract-loading-${tableId}`} />;
  }
  if (read.error !== undefined) {
    return (
      <Alert color="red" data-testid={`data-contract-error-${tableId}`}>
        {t("dataProductsTab.contract.loadError", { error: read.error })}
      </Alert>
    );
  }
  const relation = relationName({ domainId: port.domain_id, tableName: port.table_name });
  return (
    <ContractView
      contract={read.doc as OdcsContract}
      versions={read.versions as ContractVersion[]}
      tableId={tableId}
      promises={lineage ? lineage.dq.promises.filter((c) => c.checked_table_id === tableId) : null}
      consumers={
        owner && lineage ? lineage.consumers.filter((c) => c.reads.includes(relation)) : null
      }
    />
  );
}

export function ProductContracts({
  productId,
  core,
  lineage,
  owner,
  version,
}: {
  productId: string;
  core: ProductCore;
  lineage: ProductLineage | null;
  owner: boolean;
  version: number;
}) {
  const { t } = useTranslation();
  const [params] = useSearchParams();
  const contracts = coreContracts(core);
  const ports = new Map((core.tables ?? []).map((tb) => [tb.id, tb]));
  const reveal = linkedContract(contracts, params.get("contract"));
  return (
    <Collapsibles id="data-products.contracts" reveal={reveal ? "contracts" : null}>
      <CollapsibleItem
        value="contracts"
        testId={`data-product-contracts-${productId}`}
        controlTestId={`data-product-contracts-toggle-${productId}`}
        title={
          <Group gap={4} align="baseline" component="span">
            <span>{t("dataProductsTab.contract.label")}</span>
            <Text size="xs" c="var(--text-muted)" component="span">
              ({contracts.length})
            </Text>
            <Tip text={t("dataProductsTab.contract.hint")}>
              <Text
                size="xs"
                c="var(--text-muted)"
                component="span"
                style={{ cursor: "help" }}
                onClick={(e) => e.stopPropagation()}
              >
                ⓘ
              </Text>
            </Tip>
          </Group>
        }
      >
        {contracts.length === 0 ? (
          <Empty text={t("dataProductsTab.contract.empty")} />
        ) : (
          <Collapsibles id={`data-product.${productId}.contracts`} reveal={reveal}>
            {contracts.map((c) => {
              const port = ports.get(c.tableId) as CoreTable;
              return (
                <CollapsibleItem
                  key={c.tableId}
                  value={memberOf(c)}
                  defaultOpen={false}
                  mountWhenOpen
                  testId={`data-contract-item-${c.tableId}`}
                  controlTestId={`data-contract-item-${c.tableId}-toggle`}
                  title={<RowTitle contract={c} port={port} lineage={lineage} owner={owner} />}
                >
                  <OpenContract
                    productId={productId}
                    member={memberOf(c)}
                    version={version}
                    tableId={c.tableId}
                    port={port}
                    lineage={lineage}
                    owner={owner}
                  />
                </CollapsibleItem>
              );
            })}
          </Collapsibles>
        )}
      </CollapsibleItem>
    </Collapsibles>
  );
}
