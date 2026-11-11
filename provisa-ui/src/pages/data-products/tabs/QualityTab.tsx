// Copyright (c) 2026 Kenneth Stott
// Canary: 0a5d7e38-9f2c-4b61-8e4a-c1f6b3d2a908
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2157, REQ-2132: a data product's Quality tab -- its checks and their latest results. The owner
// sees its expectations on its inputs, its promises on its tables and the related checks others
// make, with the curation gaps; any other reader its promises. The owner also adds drift and
// expectation checks on each dataset's profile from the tables page's checks bar (REQ-2239).

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Badge, Group, List, Stack, Text } from "@mantine/core";
import { ListChecks, Table as TableIcon } from "lucide-react";
import { useNavigate } from "react-router-dom";
import type { ProductDq, ProductDqEntry, ProductLineage } from "../../../api/lineage";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";
import { Tip } from "../../../components/Tip";
import { ProfileChecksBar } from "../../tables/ProfileChecksBar";
import { ClaimsCallout, DqIcon, InputsList, OutcomeBadge } from "../dataProductDq";
import { COLOR, dqStatus } from "../dataProductClaims";
import { DqRulesModal } from "../DqRulesModal";
import { modelName, restModelName, type CoreTable } from "../productCore";
import { sqlExample, type TableRef } from "../tableExamples";

/** REQ-2157: the data quality section's rows, in order. */
const DQ_ROWS = ["expectations", "promises", "related"] as const;

export function QualityTab({
  productId,
  owner,
  dq,
  lineageGraph,
  outputTables,
  dqTables,
}: {
  productId: string;
  owner: boolean;
  dq: ProductDq | undefined;
  lineageGraph: ProductLineage | null;
  outputTables: CoreTable[] | undefined;
  dqTables: CoreTable[] | undefined;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [rulesFor, setRulesFor] = useState<number | null>(null);
  const shownDqRows: readonly (typeof DQ_ROWS)[number][] = owner ? DQ_ROWS : ["promises"];

  const sqlDrill = (tb: TableRef & { id: number }, testId: string) => (
    <Tip text={t("dataProductsTab.sqlQueryAria", { table: tb.tableName })} named>
      <ActionIcon
        variant="subtle"
        size="sm"
        aria-label={t("dataProductsTab.sqlQueryAria", { table: tb.tableName })}
        data-testid={testId}
        onClick={() => {
          const example = sqlExample(tb);
          navigate(example.to, { state: example.state });
        }}
      >
        <TableIcon size={14} />
      </ActionIcon>
    </Tip>
  );

  // One check in a data quality row: its claim, the checker by its model name with its results in
  // SQL and, where the product holds the checker, its rules; the table it scans; its latest run.
  const dqItem = (c: ProductDqEntry, related: boolean) => {
    const checker = {
      id: c.checker_table_id,
      domainId: c.domain_id,
      tableName: c.table_name,
      alias: c.alias,
    };
    const held = dqTables?.find((tb) => tb.id === c.checker_table_id);
    return (
      <List.Item key={c.checker_table_id} ff="monospace">
        {c.description && (
          <Text
            size="sm"
            fw={600}
            ff="text"
            data-testid={`data-product-detail-dq-claim-${c.checker_table_id}`}
          >
            {c.description}
          </Text>
        )}
        <Group gap={4} wrap="nowrap">
          <Text component="span" size="sm" ff="monospace">
            {modelName(checker)}
          </Text>
          {sqlDrill(checker, `data-product-detail-dq-sql-query-${c.checker_table_id}`)}
          {held && (
            <>
              <Tip text={t("dataProductsTab.detail.dqRulesAria", { table: c.table_name })}>
                <ActionIcon
                  variant="subtle"
                  size="sm"
                  aria-label={t("dataProductsTab.detail.dqRulesAria", { table: c.table_name })}
                  data-testid={`data-product-detail-dq-rules-${c.checker_table_id}`}
                  onClick={() => setRulesFor(c.checker_table_id)}
                >
                  <ListChecks size={14} />
                </ActionIcon>
              </Tip>
              <DqRulesModal
                opened={rulesFor === c.checker_table_id}
                onClose={() => setRulesFor(null)}
                rules={held.dqRules ?? []}
                scans={c.checked_table}
                tableName={c.table_name}
              />
            </>
          )}
        </Group>
        <Group gap={6}>
          <OutcomeBadge outcome={c.latest_outcome} />
          <Text
            c="var(--text-muted)"
            ff="text"
            size="xs"
            data-testid={`data-product-detail-dq-checks-${c.checker_table_id}`}
          >
            {t("dataProductCore.scans", { table: restModelName(c.checked_table) })}
            {related &&
              ` · ${
                c.owner_product_ids.length > 0
                  ? t("dataProductCore.assignedTo", { product: c.owner_product_ids.join(", ") })
                  : t("dataProductCore.unassigned")
              }`}
          </Text>
        </Group>
      </List.Item>
    );
  };

  if (!dq) {
    return (
      <Text size="sm" c="dimmed" data-testid={`data-product-quality-pending-${productId}`}>
        {t("dataProductsTab.detail.lineageLoading")}
      </Text>
    );
  }
  return (
    <Stack gap="sm" data-testid={`data-product-card-dataQuality-${productId}`}>
      <Text size="xs" c="dimmed">
        {t("dataProductsTab.detail.dataQualityHint")}
      </Text>
      {/* REQ-2132: the product's promises, the checks it makes on its tables. */}
      <ClaimsCallout
        kind="promises"
        checks={dq.promises}
        testId={`data-product-detail-promises-${productId}`}
      />
      {outputTables !== undefined && outputTables.length > 0 && (
        <List size="sm" data-testid={`data-product-detail-tables-${productId}`}>
          {outputTables.map((tb) => (
            <List.Item key={tb.id} ff="monospace">
              {modelName(tb)}
              <DqIcon
                checks={dq.promises}
                tableId={tb.id}
                testId={`data-product-detail-port-dq-${tb.id}`}
              />
            </List.Item>
          ))}
        </List>
      )}
      {shownDqRows.every((row) => dq[row].length === 0) &&
      (!owner || dq.unattached.length === 0) ? (
        <Text size="sm" c="dimmed">
          {t("dataProductsTab.detail.dataQualityEmpty")}
        </Text>
      ) : (
        // REQ-2157: three expandable rows -- the product's expectations on its inputs, its
        // promises on its members, and the related checks others make on either.
        <div data-testid={`data-product-detail-dq-tables-${productId}`}>
          <Collapsibles id="data-product.dq-rows">
            {shownDqRows.map((row) => {
              const status = dqStatus(dq[row]);
              return (
                <CollapsibleItem
                  key={row}
                  value={row}
                  testId={`data-product-detail-dq-${row}-${productId}`}
                  controlTestId={`data-product-detail-dq-${row}-${productId}-toggle`}
                  title={
                    <Group gap={6} wrap="nowrap">
                      <Text size="sm" fw={500}>
                        {t(`dataProductCore.dqRow_${row}`)}
                      </Text>
                      <Badge size="sm" variant="light" color="gray">
                        {dq[row].length}
                      </Badge>
                      {status && (
                        <Badge size="sm" variant="light" color={COLOR[status]}>
                          {t(`dataProductCore.status_${status}`)}
                        </Badge>
                      )}
                    </Group>
                  }
                >
                  {dq[row].length === 0 ? (
                    <Text size="sm" c="dimmed">
                      {t("dataProductCore.dqRowEmpty")}
                    </Text>
                  ) : (
                    <List size="sm">{dq[row].map((c) => dqItem(c, row === "related"))}</List>
                  )}
                </CollapsibleItem>
              );
            })}
          </Collapsibles>
        </div>
      )}
      {/* The product's own checks that scan neither an input nor a member: the owner's. */}
      {(owner ? dq.unattached : []).map((c) => (
        <Alert
          key={c.checker_table_id}
          color="yellow"
          title={t("dataProductCore.orphan")}
          data-testid={`data-product-detail-dq-${productId}-orphan-${c.checker_table_id}`}
        >
          {t("dataProductCore.orphanWarning", {
            checker: restModelName(c),
            table: restModelName(c.checked_table),
          })}
        </Alert>
      ))}
      {/* REQ-2126, REQ-2132: the owner's inputs and its expectations of them. */}
      {owner && lineageGraph && (
        <Stack gap={4} data-testid={`data-product-card-inputPorts-${productId}`}>
          <Text size="sm" fw={500}>
            {t("dataProductsTab.detail.field.inputPorts")}
          </Text>
          <ClaimsCallout
            kind="expectations"
            checks={lineageGraph.dq.expectations}
            testId={`data-product-detail-expectations-${productId}`}
          />
          <InputsList
            inputs={lineageGraph.inputs}
            expectations={lineageGraph.dq.expectations}
            testId={`data-product-detail-inputs-${productId}`}
          />
        </Stack>
      )}
      {/* REQ-1934: the owner adds checks on each dataset's profile from the tables page's bar. */}
      {owner && outputTables !== undefined && outputTables.length > 0 && (
        <Collapsibles id={`data-product.profile-checks.${productId}`}>
          {outputTables.map((tb) => (
            <CollapsibleItem
              key={tb.id}
              value={String(tb.id)}
              defaultOpen={false}
              mountWhenOpen
              controlTestId={`data-product-profile-checks-${tb.id}-toggle`}
              title={
                <Text size="sm">
                  {t("dataProductOffers.profileChecks", { table: modelName(tb) })}
                </Text>
              }
            >
              <ProfileChecksBar tableId={tb.id} />
            </CollapsibleItem>
          ))}
        </Collapsibles>
      )}
    </Stack>
  );
}
