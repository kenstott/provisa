// Copyright (c) 2026 Kenneth Stott
// Canary: ff511d1f-2c93-40ca-84b7-d92d048c2720
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a product member's contract, its output port, as its open row in the product's contracts
// list shows it: every ODCS 3.1 element, declared or derived (REQ-2118), in the shared contract
// layout (ContractLayout) -- details, versions and changes, pricing, custom properties and tags;
// roles, SLAs and freshness, support and team, version history; how to use, columns, quality promises.
// A contract is what a product's user reads, so it never says who consumes the port: that is the
// owner's, on the product (REQ-2160). Elements are named by their ODCS names, the standard's own
// vocabulary, and what nothing provides is named as not declared.

import { useTranslation } from "react-i18next";
import { Anchor, Badge, Group, Stack, Text } from "@mantine/core";
import { Link } from "react-router-dom";
import type { OdcsContract, OdcsRecord } from "../../api/dataProductContracts";
import type { ContractVersion } from "../../types/admin";
import type { ProductDqEntry } from "../../api/lineage";
import {
  ContractLayout,
  ContractPanel,
  NotDeclared,
  type PanelColumn,
  type PanelTable,
} from "./ContractLayout";
import { canRead, contractGrant, surfaceAddress, type Grant } from "./howToUse";
import { PromisesTable } from "./PromisesTable";
import {
  graphqlExample,
  grpcExample,
  jsonApiExample,
  restExample,
  sqlExample,
  type ExampleLink,
  type TableRef,
} from "./tableExamples";
import { useSparqlExample } from "./useSparqlExample";
import type { MetaReader } from "../../api/metaGraph";

const columns = (...names: string[]): PanelColumn[] => names.map((name) => ({ name }));

/** What a property promises of its values, in one column so a wide schema fits its panel. */
function propertyConstraints(p: OdcsRecord): string[] {
  const position = p.primaryKeyPosition;
  return [
    ...(p.required === true ? ["required"] : []),
    ...(p.unique === true ? ["unique"] : []),
    ...(p.primaryKey === true ? [position ? `primaryKey ${String(position)}` : "primaryKey"] : []),
  ];
}

/** A property's elements as ODCS 3.1 names them; any further element it carries follows them. */
function propertyColumns(constraints: string): PanelColumn[] {
  return [
    { name: "name", whole: true },
    ...columns("logicalType", "physicalType", "logicalTypeOptions"),
    {
      name: "constraints",
      label: constraints,
      reads: ["required", "unique", "primaryKey", "primaryKeyPosition"],
      value: propertyConstraints,
    },
    ...columns("classification", "description", "examples", "quality"),
  ];
}

/** The saved versions, newest first: when and by whom each was saved, whether it breaks a consumer
 * and its differences from the one before it. */
function useVersionsTable(versions: ContractVersion[], tableId: number): PanelTable {
  const { t, i18n } = useTranslation();
  const rowId = (version: unknown) => `data-contract-version-row-${tableId}-${String(version)}`;
  return {
    name: "versions",
    testId: `data-contract-history-${tableId}`,
    rowTestId: (row) => rowId(row.version),
    rows: [...versions].reverse().map((v) => ({
      version: v.version,
      saved_at: v.saved_at,
      saved_by: v.saved_by,
      changes: v.changes,
    })),
    columns: [
      {
        name: "version",
        reads: ["version"],
        cell: (row) => (
          <Stack gap={2} align="flex-start">
            <Text size="sm" style={{ whiteSpace: "nowrap" }}>
              {t("dataProductsTab.contract.versionBadge", { version: row.version })}
            </Text>
            {(row.changes as ContractVersion["changes"]).some((c) => c.breaking) && (
              <Badge
                size="xs"
                variant="light"
                color="red"
                data-testid={`${rowId(row.version)}-breaking`}
              >
                {t("dataProductsTab.contract.breaking")}
              </Badge>
            )}
          </Stack>
        ),
      },
      {
        name: "saved",
        label: t("dataProductsTab.contract.versionSaved"),
        reads: ["saved_at", "saved_by"],
        value: (row) => row.saved_at,
        cell: (row) => (
          <span data-testid={`${rowId(row.version)}-saved`}>
            {t("dataProductsTab.contract.savedBy", {
              when: new Date(String(row.saved_at)).toLocaleString(i18n.language),
              who: row.saved_by ?? t("dataProductsTab.contract.savedBySystem"),
            })}
          </span>
        ),
      },
      {
        name: "changes",
        label: t("dataProductsTab.contract.versionChanges"),
        // A version with nothing to compare still says so, so the column is always drawn.
        value: () => true,
        cell: (row) => <VersionChanges id={rowId(row.version)} row={row} />,
      },
    ],
  };
}

/** Who reads a contract: the roles it holds, and its acting roles as a request names them. */
export interface ContractViewer {
  held: ReadonlySet<string>;
  acting: string | null;
}

/** Each surface's example of a table; a surface with none has no link. Flight SQL has no page of
 * its own and opens SQL, the language it speaks. */
function useExamples(
  tableId: number,
  table: TableRef,
  reader: MetaReader | null,
  acting: string | null,
  sparql: boolean,
): Record<string, ExampleLink | null> {
  const sparqlLink = useSparqlExample(tableId, acting, sparql);
  const sql = sqlExample(table);
  const rest = restExample(reader, table);
  return {
    pgwire: sql,
    "flight-sql": sql,
    grpc: grpcExample(reader, table),
    graphql: graphqlExample(reader, table),
    rest,
    openapi: rest,
    jsonapi: jsonApiExample(table),
    sparql: sparqlLink,
  };
}

/** The sentence every surface carries: it works once access is granted, and who grants it. */
function useAccessSentence({ roles, approvers }: Grant): string {
  const { t } = useTranslation();
  const key = "dataProductsTab.contract.howToUse.access";
  const named = { roles: roles.join(", "), approvers: approvers.join(", ") };
  if (roles.length === 0) return t(`${key}.undeclared`);
  return t(`${key}.${approvers.length > 0 ? "granted" : "noApprover"}`, named);
}

/** Who a viewer who cannot read the table asks, in place of the link. */
function AskForAccess({ grant, testId }: { grant: Grant; testId: string }) {
  const { t } = useTranslation();
  const key = "dataProductsTab.contract.howToUse.ask";
  const { roles, approvers } = grant;
  return (
    <Text size="sm" data-testid={testId}>
      {approvers.length > 0
        ? t(`${key}.approvers`, { approvers: approvers.join(", ") })
        : roles.length > 0
          ? t(`${key}.roles`, { roles: roles.join(", ") })
          : t(`${key}.nobody`)}
    </Text>
  );
}

/** How to use: each surface's address for this table, how it is used, and its example. */
function useHowToUseTable(
  contract: OdcsContract,
  tableId: number,
  table: TableRef,
  reader: MetaReader | null,
  viewer: ContractViewer,
): PanelTable {
  const { t } = useTranslation();
  const key = "dataProductsTab.contract.howToUse";
  const servers = contract.servers ?? [];
  const grant = contractGrant(contract);
  const readable = canRead(grant, viewer.held);
  const access = useAccessSentence(grant);
  const examples = useExamples(
    tableId,
    table,
    reader,
    viewer.acting,
    readable && servers.some((s) => s.server === "sparql"),
  );
  const rowId = (row: OdcsRecord) => `data-contract-how-to-use-${tableId}-${String(row.server)}`;
  const surface = (row: OdcsRecord) => t(`${key}.surface.${String(row.server)}`);
  return {
    name: "servers",
    testId: `data-contract-how-to-use-${tableId}`,
    rowTestId: rowId,
    rows: servers,
    columns: [
      {
        name: "surface",
        label: t(`${key}.column.surface`),
        // The server's own fields are what this table is drawn from, never further columns.
        reads: [...new Set(servers.flatMap((server) => Object.keys(server)))],
        value: surface,
        whole: true,
      },
      {
        name: "address",
        label: t(`${key}.column.address`),
        reads: [],
        value: surfaceAddress,
        whole: true,
      },
      {
        name: "how",
        label: t(`${key}.column.how`),
        reads: [],
        value: () => true,
        cell: (row) => (
          <Stack gap={2}>
            <Text size="sm">
              {t(`${key}.how.${String(row.server)}`, {
                database: row.database,
                schema: row.schema,
                service: row.serviceName,
              })}
            </Text>
            <Text size="xs" c="dimmed" data-testid={`${rowId(row)}-access`}>
              {access}
            </Text>
          </Stack>
        ),
      },
      {
        name: "link",
        label: t(`${key}.column.link`),
        reads: [],
        value: () => true,
        cell: (row) => {
          if (!readable) return <AskForAccess grant={grant} testId={`${rowId(row)}-ask`} />;
          const example = examples[String(row.server)];
          if (!example) return t("detailFields.empty");
          return (
            <Anchor
              component={Link}
              size="sm"
              to={example.to}
              state={example.state}
              aria-label={t(`${key}.openAria`, { surface: surface(row), table: table.tableName })}
              data-testid={`${rowId(row)}-link`}
              style={{ whiteSpace: "nowrap" }}
            >
              {t(`${key}.open`)}
            </Anchor>
          );
        },
      },
    ],
  };
}

function VersionChanges({ id, row }: { id: string; row: OdcsRecord }) {
  const { t } = useTranslation();
  const changes = row.changes as ContractVersion["changes"];
  if (changes.length === 0) {
    return (
      <Text
        size="sm"
        c="dimmed"
        className="contract-version-changes"
        data-testid={`${id}-no-changes`}
      >
        {t("dataProductsTab.contract.noChanges")}
      </Text>
    );
  }
  return (
    <Stack gap={2} className="contract-version-changes" data-testid={`${id}-changes`}>
      {changes.map((c, n) => (
        <Group key={n} gap={6} wrap="nowrap" align="baseline" justify="space-between">
          <Text size="sm" style={{ minWidth: 0, overflowWrap: "anywhere" }}>
            {t(`dataProductsTab.contract.change.${c.kind}`, {
              // A term of use is named in the reader's words; every other subject is a name.
              subject:
                c.kind === "terms_of_use_changed"
                  ? t(`dataProductOffers.term_${c.subject}`)
                  : c.subject,
              before: c.before,
              after: c.after,
            })}
          </Text>
          {c.breaking && (
            <Badge size="xs" variant="light" color="red" style={{ flexShrink: 0 }}>
              {t("dataProductsTab.contract.breaking")}
            </Badge>
          )}
        </Group>
      ))}
    </Stack>
  );
}

export function ContractView({
  contract,
  versions,
  tableId,
  promises,
  table,
  reader,
  viewer,
}: {
  contract: OdcsContract;
  /** Its saved versions, oldest first (REQ-2158). */
  versions: ContractVersion[];
  tableId: number;
  /** The product's promises on this port (REQ-2132); null while the product's lineage is read. */
  promises: ProductDqEntry[] | null;
  /** The port's table, which its examples name. */
  table: TableRef;
  /** The acting role's schema and root fields; null while they are read. */
  reader: MetaReader | null;
  viewer: ContractViewer;
}) {
  const { t } = useTranslation();
  const history = useVersionsTable(versions, tableId);
  const howToUse = useHowToUseTable(contract, tableId, table, reader, viewer);
  const status = t(`dataProductsTab.contract.status.${contract.status}`);
  const object = contract.schema?.[0];
  const price = contract.price;
  const custom = contract.customProperties ?? [];
  const panel = (key: string, label = `card.${key}`) => ({
    section: `contract.${key}`,
    testId: `data-contract-${tableId}-${key}`,
    label: t(`dataProductsTab.contract.${label}`),
  });
  return (
    <ContractLayout
      testId={`data-contract-${tableId}`}
      small={
        <>
          <ContractPanel
            {...panel("details")}
            fields={[
              { name: "id", value: contract.id },
              { name: "name", value: contract.name },
              { name: "status", value: status },
              { name: "domain", value: contract.domain },
              { name: "dataProduct", value: contract.dataProduct },
              { name: "tenant", value: contract.tenant },
              { name: "purpose", value: contract.description?.purpose },
              { name: "limitations", value: contract.description?.limitations },
              { name: "usage", value: contract.description?.usage },
            ]}
          />
          <ContractPanel
            {...panel("versions")}
            fields={[
              { name: "version", value: contract.version },
              { name: "status", value: status },
              { name: "contractCreatedTs", value: contract.contractCreatedTs },
            ]}
          />
          <ContractPanel
            {...panel("pricing", "section.pricing")}
            fields={[
              { name: "priceAmount", value: price?.priceAmount },
              { name: "priceCurrency", value: price?.priceCurrency },
              { name: "priceUnit", value: price?.priceUnit },
            ]}
          />
          <ContractPanel
            {...panel("custom", "section.custom")}
            fields={[
              ...(custom.length > 0
                ? custom.map((c) => ({ name: c.property, value: c.value }))
                : [{ name: "customProperties", value: undefined }]),
              { name: "tags", value: contract.tags },
            ]}
          />
        </>
      }
      medium={
        <>
          <ContractPanel
            {...panel("roles")}
            tables={[
              {
                name: "roles",
                rows: contract.roles ?? [],
                columns: columns("role", "access"),
                testId: `data-contract-roles-${tableId}`,
              },
            ]}
          />
          <ContractPanel
            {...panel("sla")}
            fields={[{ name: "slaDefaultElement", value: contract.slaDefaultElement }]}
            tables={[
              {
                name: "slaProperties",
                rows: contract.slaProperties ?? [],
                columns: columns("property", "value", "unit", "description"),
                testId: `data-contract-sla-${tableId}`,
              },
            ]}
          />
          <ContractPanel
            {...panel("support", "section.support")}
            fields={[
              { name: "team.name", value: contract.team?.name },
              { name: "team.description", value: contract.team?.description },
            ]}
            tables={[
              {
                name: "support",
                rows: contract.support ?? [],
                columns: columns("channel", "url", "tool", "description"),
                testId: `data-contract-support-${tableId}`,
              },
              {
                name: "team.members",
                rows: contract.team?.members ?? [],
                columns: columns("username", "role"),
                testId: `data-contract-team-${tableId}`,
              },
            ]}
          />
          <ContractPanel {...panel("history")} tables={[history]} />
        </>
      }
      large={
        <>
          <ContractPanel {...panel("howToUse", "section.howToUse")} wide tables={[howToUse]} />
          <ContractPanel
            {...panel("fields")}
            wide
            fields={[
              { name: "name", value: object?.name },
              { name: "physicalName", value: object?.physicalName },
              { name: "physicalType", value: object?.physicalType },
              { name: "description", value: object?.description },
              { name: "businessName", value: object?.businessName },
              { name: "dataGranularityDescription", value: object?.dataGranularityDescription },
              { name: "tags", value: object?.tags },
            ]}
            tables={[
              {
                name: "properties",
                rows: object?.properties ?? [],
                columns: propertyColumns(t("dataProductsTab.contract.constraints")),
                testId: `data-contract-columns-${tableId}`,
                rowTestId: (row) => `data-contract-columns-${tableId}-${String(row.name)}`,
              },
              {
                name: "relationships",
                rows: (object?.relationships ?? []) as OdcsRecord[],
                columns: columns("type", "from", "to"),
                testId: `data-contract-relationships-${tableId}`,
              },
            ]}
          />
          <ContractPanel {...panel("quality")} wide>
            {/* REQ-2132: the product's promises on this port, as its Output Ports card shows them. */}
            {promises && promises.length > 0 ? (
              <div data-testid={`data-contract-promises-${tableId}`}>
                <PromisesTable
                  promises={promises}
                  testId={`data-contract-promises-table-${tableId}`}
                />
              </div>
            ) : (
              <NotDeclared names={["quality"]} />
            )}
          </ContractPanel>
        </>
      }
    />
  );
}
