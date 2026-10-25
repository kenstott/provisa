// Copyright (c) 2026 Kenneth Stott
// Canary: 6d1f8b39-2e74-4a05-c9b8-a3e5d7f2c816
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2132, REQ-2157: a data product's quality claims, as its lineage gives them (the product
// lineage endpoint's dq: each checker table's description and latest run). Output Ports carry the product's Promises, Inputs
// its Expectations: each a count, a summary status and a list one click away, every row opening the
// checker's results. A dataset with checks shows a DQ icon coloured by their latest result; other
// products' (or no product's) checks are listed as partner DQ, and the product's own checks that
// scan neither an input nor an output port as orphans.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Alert, Badge, Group, List, Modal, Table, Text, UnstyledButton } from "@mantine/core";
import { ShieldCheck } from "lucide-react";
import { Tip } from "../../components/Tip";
import { tableSqlRef } from "../../naming";
import type { ProductInput } from "../../api/lineage";
import { modelName, type CoreCheck } from "./productCore";
import { checksIn, dqStatus, type DqStatus } from "./dataProductClaims";

const COLOR: Record<DqStatus, string> = { passing: "green", failing: "red", notRun: "gray" };

/** The DQ icon of a dataset: its checks' latest result. Nothing when it has no checks. */
export function DqIcon({
  checks,
  tableId,
  testId,
}: {
  checks: CoreCheck[];
  tableId: number | null | undefined;
  testId: string;
}) {
  const { t } = useTranslation();
  const mine = checks.filter((c) => c.checked_table_id === tableId);
  if (tableId == null || mine.length === 0) return null;
  const status = dqStatus(mine);
  const label = status
    ? t(`dataProductCore.dqIcon_${status}`, { count: mine.length })
    : t("dataProductCore.dqIcon", { count: mine.length });
  return (
    <Tip text={label}>
      <ShieldCheck
        size={14}
        aria-label={label}
        color={status ? `var(--mantine-color-${COLOR[status]}-6)` : "var(--text-muted)"}
        data-testid={testId}
        data-status={status ?? undefined}
        style={{ verticalAlign: "middle", marginInlineStart: 4 }}
      />
    </Tip>
  );
}

const datasetOf = (c: CoreCheck) => (c.checked_table ? modelName(c.checked_table) : "");

function ClaimRows({ checks, testId }: { checks: CoreCheck[]; testId: string }) {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  return (
    <Table striped data-testid={testId}>
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("dataProductCore.claim")}</Table.Th>
          <Table.Th>{t("dataProductCore.datasetsCovered")}</Table.Th>
          <Table.Th>{t("dataProductCore.latestOutcome")}</Table.Th>
          <Table.Th>{t("dataProductCore.ranAt")}</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {checks.map((c) => {
          const outcome = c.checker?.latest_outcome;
          const ran = c.checker?.latest_run_at;
          const results = c.checker
            ? tableSqlRef({
                domainId: c.checker.domain_id,
                tableName: c.checker.table_name,
                alias: c.checker.alias,
              })
            : null;
          const open = () =>
            results &&
            navigate("/sql", { state: { sql: `select * from ${results}`, autoRun: true } });
          return (
            <Table.Tr
              key={c.id}
              onClick={open}
              style={{ cursor: "pointer" }}
              data-testid={`${testId}-row-${c.checker_table_id}`}
            >
              <Table.Td>
                <Text size="sm" fw={500}>
                  {c.checker?.description || (c.checker && modelName(c.checker))}
                </Text>
                <Text size="xs" c="var(--text-muted)" ff="monospace">
                  {c.checker ? modelName(c.checker) : c.checker_table_id}
                </Text>
              </Table.Td>
              <Table.Td ff="monospace">{datasetOf(c)}</Table.Td>
              <Table.Td>
                {outcome === undefined ? (
                  ""
                ) : (
                  <Badge
                    size="sm"
                    color={outcome === "pass" ? "green" : outcome === "fail" ? "red" : "gray"}
                  >
                    {t(`dataProductCore.outcome_${outcome ?? "none"}`)}
                  </Badge>
                )}
              </Table.Td>
              <Table.Td>
                {ran ? new Date(ran).toLocaleString(i18n.language) : outcome === null ? "—" : ""}
              </Table.Td>
            </Table.Tr>
          );
        })}
      </Table.Tbody>
    </Table>
  );
}

/** Promises (on Output Ports) or Expectations (on Inputs): the count and the summary status, and
 * the list one click away. */
export function ClaimsCallout({
  kind,
  checks,
  testId,
}: {
  kind: "promises" | "expectations";
  checks: CoreCheck[];
  testId: string;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  if (checks.length === 0) return null;
  const status = dqStatus(checks);
  return (
    <>
      <UnstyledButton
        onClick={() => setOpen(true)}
        className="dp-claims"
        data-testid={testId}
        data-status={status ?? undefined}
      >
        <Group gap={6} wrap="nowrap">
          <ShieldCheck
            size={14}
            color={status ? `var(--mantine-color-${COLOR[status]}-6)` : "var(--text-muted)"}
          />
          <Text size="sm" fw={500}>
            {t(`dataProductCore.${kind}`, { count: checks.length })}
          </Text>
          {status && (
            <Badge size="sm" variant="light" color={COLOR[status]}>
              {t(`dataProductCore.status_${status}`)}
            </Badge>
          )}
        </Group>
      </UnstyledButton>
      <Modal
        opened={open}
        onClose={() => setOpen(false)}
        title={t(`dataProductCore.${kind}Title`)}
        size="xl"
        data-testid={`${testId}-modal`}
      >
        <ClaimRows checks={checks} testId={`${testId}-list`} />
      </Modal>
    </>
  );
}

/** Other products' checks (or no product's) on this product's ports and inputs, and this
 * product's own checks that scan neither: listed apart, so they never count as its claims. */
export function PartnersAndOrphans({ checks, testId }: { checks: CoreCheck[]; testId: string }) {
  const { t } = useTranslation();
  const partners = checksIn(checks, "partner_dq");
  const orphans = checksIn(checks, "orphan");
  return (
    <>
      {partners.length > 0 && (
        <div data-testid={`${testId}-partners`}>
          <Text c="dimmed" fw={500} size="xs" tt="uppercase" mt="sm">
            {t("dataProductCore.partnerDq")}
          </Text>
          <List size="sm">
            {partners.map((c) => (
              <List.Item key={c.id}>
                {c.checker?.description || (c.checker && modelName(c.checker))}
                <Text component="span" c="var(--text-muted)" size="xs" ff="monospace">
                  {" "}
                  {datasetOf(c)} ·{" "}
                  {c.assigned_product_id
                    ? t("dataProductCore.assignedTo", { product: c.assigned_product_id })
                    : t("dataProductCore.unassigned")}
                </Text>
              </List.Item>
            ))}
          </List>
        </div>
      )}
      {orphans.map((c) => (
        <Alert
          key={c.id}
          color="yellow"
          mt="xs"
          title={t("dataProductCore.orphan")}
          data-testid={`${testId}-orphan-${c.checker_table_id}`}
        >
          {t("dataProductCore.orphanWarning", {
            checker: c.checker ? modelName(c.checker) : "",
            table: datasetOf(c),
          })}
        </Alert>
      ))}
    </>
  );
}

/** An input as the panel names it: a registered table by its model name; the source a member is
 *  read from by its source and where in it (source/schema/table). */
function inputName(input: ProductInput): string {
  if (input.kind !== "source") return modelName(input);
  return [input.source_id, input.source_schema, input.source_table].filter(Boolean).join("/");
}

/** REQ-2126: the product's nearest physical inputs, each with its expectations' DQ icon. */
export function InputsList({
  inputs,
  checks,
  testId,
}: {
  inputs: ProductInput[];
  checks: CoreCheck[];
  testId: string;
}) {
  const { t } = useTranslation();
  if (inputs.length === 0) {
    return (
      <Text size="sm" c="var(--text-muted)">
        {t("dataProductCore.inputsEmpty")}
      </Text>
    );
  }
  const expectations = checksIn(checks, "expectation");
  return (
    <List size="sm" data-testid={testId}>
      {inputs.map((input) => (
        <List.Item key={input.relation} ff="monospace">
          {inputName(input)}
          {input.kind === "source" && input.source_type && (
            <Text component="span" c="var(--text-muted)" ff="text" size="xs">
              {" "}
              ({input.source_type})
            </Text>
          )}
          {input.kind !== "source" && (
            <DqIcon
              checks={expectations}
              tableId={input.table_id}
              testId={`${testId}-dq-${input.table_id}`}
            />
          )}
        </List.Item>
      ))}
    </List>
  );
}
