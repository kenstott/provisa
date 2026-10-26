// Copyright (c) 2026 Kenneth Stott
// Canary: 0f62c0c5-a9e2-4f0c-812f-d84dd160d2c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2127: the Data Product detail panel. Its core -- the product's fields, member tables,
// commands, tags, glossary terms, data-quality and profile evidence, related tables and lineage --
// is read from the meta domain as the signed-in user (productCore.ts); a part the role cannot
// read there is absent. Drilling down opens the detail where it lives: a member table's rows, a
// checker's or profile run's results, read in their own domains as the same user.

import { DetailFields, type DetailField } from "../../components/detail/DetailFields";
import React, { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Alert, Badge, Group, List, Loader, Modal, Stack, Text } from "@mantine/core";
import {
  Braces,
  Cable,
  CircleHelp,
  FileJson,
  Globe,
  ListChecks,
  Pencil,
  Share2,
  Table as TableIcon,
  Trash2,
} from "lucide-react";
import type { DataProduct } from "../../types/admin";
import { DqRulesModal } from "./DqRulesModal";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import { tableSqlRef } from "../../naming";
import { Tip } from "../../components/Tip";
import { LineageDag } from "../../components/lineage/LineageDag";
import type { DescribeColumn } from "../../components/lineage/column-descriptions";
import type { ProductDq, ProductDqEntry, ProductLineage } from "../../api/lineage";
import { rootKey, type MetaReader } from "../../api/metaGraph";
import { GraphPanel } from "../../components/graph3d/GraphPanel";
import { ClampedDetail } from "./ClampedDetail";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";
import {
  coreGraph,
  modelName,
  type CoreEdge,
  type CoreTable,
  type ProductCore,
} from "./productCore";
import { ClaimsCallout, DqIcon, InputsList, OutcomeBadge } from "./dataProductDq";
import { COLOR, dqStatus } from "./dataProductClaims";

/** REQ-2157: the data quality section's rows, in order. */
const DQ_ROWS = ["expectations", "promises", "related"] as const;

interface TableRef {
  domain_id: string;
  table_name: string;
  alias: string | null;
}

const sqlRefOf = (tb: TableRef) =>
  tableSqlRef({ domainId: tb.domain_id, tableName: tb.table_name, alias: tb.alias });
const selectAll = (tb: TableRef) => `select * from ${sqlRefOf(tb)}`;

/** A table's root query field in the acting role's schema (from /data/graph-schema). */
function rootOf(reader: MetaReader | null, tb: TableRef): string | null {
  return reader?.roots.get(rootKey(tb.domain_id, tb.table_name)) ?? null;
}

// REQ-1634: one example query per member table, by the root field and the columns the role's own
// schema gives it -- the compiler's names, never re-derived here.
function graphqlExample(reader: MetaReader, field: string): string | null {
  const type = reader.schema.rootType(field);
  const columns = type ? reader.schema.scalars(type) : [];
  return columns.length > 0 ? `{ ${field} { ${columns.join(" ")} } }` : null;
}

// Mirrors provisa.compiler.naming.to_type_name + provisa.grpc.proto_gen._to_proto_type_name — both
// pure functions of the field name alone. sa__userByName -> Sa__UserByName -> SaUserByName.
function grpcMethod(fieldName: string): string {
  let typeName: string;
  if (fieldName.includes("__")) {
    const [prefix, rest] = fieldName.split("__", 2);
    const prefixPascal = prefix ? prefix[0].toUpperCase() + prefix.slice(1).toLowerCase() : "";
    const restPascal = rest ? rest[0].toUpperCase() + rest.slice(1) : "";
    typeName = prefixPascal + restPascal;
  } else {
    typeName = fieldName[0].toUpperCase() + fieldName.slice(1);
  }
  return `Query${typeName}()`;
}

// A zero-width space after "/" gives the browser a break opportunity there — plain "/" doesn't
// wrap on its own, so a long gqlName/cqlName pair would otherwise overflow its monospace panel.
function joinAliasPair(names: (string | null | undefined)[]): string {
  return names.filter(Boolean).join("/​");
}

interface DataProductDetailPanelProps {
  p: DataProduct;
  /** The meta-domain core of the panel; null while it is read. */
  core: ProductCore | null;
  coreError: string | null;
  /** The acting role's schema and root fields, for the member tables' drill-down queries. */
  reader: MetaReader | null;
  // REQ-1634: gates Edit/Delete.
  canEdit: boolean;
  /** REQ-2160: the reader holds the product's owner or team role. The owner also sees its
   *  expectations, related and unattached checks, and its input ports; any other reader sees its
   *  promises. The split hides lists and cards only, never a right: Edit and Delete follow
   *  canEdit. What either view is shown is already cut to the reader's rights (REQ-2152). */
  owner: boolean;
  lineageLoading: boolean;
  lineageError: string;
  // REQ-1640, REQ-2156: the product's member tables, their full upstream ancestry and one hop
  // downstream, with its nearest physical inputs, cut to what the reader may read (the product
  // lineage endpoint) -- rendered with the same LineageDag component the Lineage page uses
  // (REQ-1161/1627). Any reader of the product sees it.
  lineageGraph: ProductLineage | null;
  // REQ-1667: the member relations the DAG's swimlanes are built around (lane 0).
  lineageMembers?: ReadonlySet<string>;
  // A field's registered description for the lineage hover, over every table (not just members).
  describeColumn?: DescribeColumn;
  // A lineage relation as the catalog names its table.
  relationLabel?: (relation: string) => string;
  onEdit: () => void;
  onDelete: () => void;
}

// A card in the masonry: kept whole within its column.
const CARD_IN_COLUMN: React.CSSProperties = {
  breakInside: "avoid",
  minWidth: 0,
  marginBottom: "var(--mantine-spacing-sm)",
};
// A card's content stays inside the card: long words wrap, and what cannot wrap (a table's
// columns) scrolls within it.
const CARD_BODY: React.CSSProperties = { minWidth: 0, overflowX: "auto", overflowWrap: "anywhere" };

/** One card of the panel: the application's collapsible (one look, its open or closed state
 * remembered per section), headed by its label and, behind (?), what it holds. ``wide`` spans the
 * panel's width, outside the columns. */
function DetailCard({
  section,
  label,
  hint,
  testId,
  tourId,
  wide = false,
  children,
}: {
  section: string;
  label: string;
  hint?: string;
  testId: string;
  tourId?: string;
  wide?: boolean;
  children: React.ReactNode;
}) {
  const heading = (
    <Group gap="xs" wrap="nowrap" component="span">
      <span>{label}</span>
      {hint && (
        <Tip text={hint}>
          <ActionIcon
            component="span"
            variant="subtle"
            color="gray"
            size="xs"
            role="button"
            tabIndex={0}
            aria-label={hint}
            data-testid={`${testId}-hint`}
            // The icon sits in the heading; using it must not also open or close the card.
            onClick={(e) => e.stopPropagation()}
            onKeyDown={(e) => e.stopPropagation()}
          >
            <CircleHelp size={14} aria-hidden />
          </ActionIcon>
        </Tip>
      )}
    </Group>
  );
  return (
    <div style={wide ? undefined : CARD_IN_COLUMN} data-tour={tourId} data-testid={testId}>
      <Collapsibles id={`data-product.${section}`}>
        <CollapsibleItem value={section} title={heading} controlTestId={`${testId}-toggle`}>
          <div style={CARD_BODY}>{children}</div>
        </CollapsibleItem>
      </Collapsibles>
    </div>
  );
}

function Empty({ text }: { text: string }) {
  return (
    <Text size="sm" c="var(--text-muted)">
      {text}
    </Text>
  );
}

function Muted({ children }: { children: React.ReactNode }) {
  return (
    <Text component="span" c="var(--text-muted)" ff="text" size="xs">
      {" "}
      {children}
    </Text>
  );
}

function relatedLabel(e: CoreEdge): string {
  return e.target_table ? modelName(e.target_table) : String(e.target_table_id);
}

// Row-click detail view for a data product — same detail-then-edit pattern as
// MetricDetailPanel (REQ-1323): the read-only facts with Edit/Delete inside.
export function DataProductDetailPanel({
  p,
  core,
  coreError,
  reader,
  canEdit,
  owner,
  lineageLoading,
  lineageError,
  lineageGraph,
  lineageMembers,
  describeColumn,
  relationLabel,
  onEdit,
  onDelete,
}: DataProductDetailPanelProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // REQ-1640/REQ-1627: expanded by default, same as statement lineage's initial view — this
  // subgraph (member tables + one relation-hop) is small like a single query's lineage, not
  // federation-scale, so it reads better column-by-column with the real role colours than
  // collapsed to indigo dataset boxes.
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  // REQ-1667: the same graph at 90% of the viewport; collapse state is shared with the inline one.
  const [lineageModalOpen, setLineageModalOpen] = useState(false);
  // REQ-1667: lane 0 is the product's published surface; the others are named by distance.
  const laneTitle = useCallback(
    (lane: number) =>
      lane === 0
        ? t("dataProductsTab.tablesLabel")
        : lane === -1
          ? t("dataProductsTab.detail.field.inputPorts")
          : lane > 0
            ? t("dataProductsTab.detail.lineageLaneDownstream", { n: lane })
            : t("dataProductsTab.detail.lineageLaneUpstream", { n: -lane }),
    [t],
  );
  const toggleRelation = (relation: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (!next.delete(relation)) next.add(relation);
      return next;
    });
  // REQ-1443 clause 10: the checker table whose rules modal is open, by table id.
  const [rulesFor, setRulesFor] = useState<number | null>(null);

  // REQ-1634: Cypher click-through only for relationships that resolve to a usable type and
  // aren't opted out — mirrors GraphPage's handleRelClick input contract.
  // The relationships between the product's member tables, under the names meta stores on each.
  const memberIds = new Set((core?.tables ?? []).map((tb) => tb.id));
  const internalRelationships = (core?.tables ?? []).flatMap((tb) =>
    (tb.source_relationships ?? [])
      .filter((r) => r.target_table != null && memberIds.has(r.target_table.id))
      .map((r) => ({ ...r, source: tb })),
  );
  const cypherRows = internalRelationships
    .filter((r) => !r.disable_cypher)
    .map((r) => ({ r, type: r.cypher_type }));

  // Masonry panel grid: a CSS multi-column layout packs each card into whichever column has room
  // next, instead of flex-wrap's row-by-row layout (which pads every card in a row up to the row's
  // tallest neighbor, leaving the shorter cards full of unused whitespace).
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
  // table it scans is — its rows are the product's quality evidence, not part of its published
  // surface, so it is listed under Data Quality rather than among the output ports.
  const outputTables = core.tables?.filter((tb) => tb.dq_contract == null);
  const dqTables = core.tables?.filter((tb) => tb.dq_contract != null);
  const profiled = outputTables?.filter((tb) => (tb.profile_results ?? []).length > 0);
  // The graph card is drawn from this read, as every other card is.
  const graph = coreGraph(core);
  // REQ-2126: the tables with a direct declared relationship to a member, not themselves members;
  // the indirect relationship rows are the product graph's, not the panel's.
  const related = core.edges?.filter((e) => e.kind === "related");
  // REQ-2157: the product's checks, from its lineage as its reader may read it (REQ-2156).
  const dq: ProductDq | undefined = lineageGraph?.dq;
  // REQ-2160: the owner sees every row; any other reader the product's promises.
  const shownDqRows: readonly (typeof DQ_ROWS)[number][] = owner ? DQ_ROWS : ["promises"];
  const customPropertyEntries = Object.entries(product.customProperties ?? {});

  // Every value of the product, under the labels its form gives them.
  const fields: DetailField[] = [
    { label: t("dataProductsTab.idLabel"), value: product.id },
    { label: t("dataProductsTab.nameLabel"), value: product.name },
    { label: t("dataProductsTab.domainLabel"), value: product.domainId },
    { label: t("dataProductsTab.versionLabel"), value: product.version },
    { label: t("dataProductsTab.statusLabel"), value: product.status },
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
    { label: t("dataProductsTab.slaLabel"), value: product.sla },
    { label: t("dataProductsTab.purposeLabel"), value: product.purpose },
    { label: t("dataProductsTab.limitationsLabel"), value: product.limitations },
    { label: t("dataProductsTab.usageLabel"), value: product.usage },
  ];

  const sqlDrill = (tb: TableRef & { id: number }, testId: string) => (
    <Tip text={t("dataProductsTab.sqlQueryAria", { table: tb.table_name })} named>
      <ActionIcon
        variant="subtle"
        size="sm"
        aria-label={t("dataProductsTab.sqlQueryAria", { table: tb.table_name })}
        data-testid={testId}
        onClick={() => navigate("/sql", { state: { sql: selectAll(tb), autoRun: true } })}
      >
        <TableIcon size={14} />
      </ActionIcon>
    </Tip>
  );

  // One check in a data quality row: its claim, the checker by its model name with its results
  // in SQL and, where the product holds the checker, its rules; the table it scans; its latest run.
  // A related check names the product owning it, or that none does.
  const dqItem = (c: ProductDqEntry, related: boolean) => {
    const checker = {
      id: c.checker_table_id,
      domain_id: c.domain_id,
      table_name: c.table_name,
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
          {/* The scan results are reached through SQL alone here — the other access patterns
              belong to the output ports, not to the evidence. */}
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
                rules={held.dq_rules ?? []}
                scans={c.checked_table}
                tableName={c.table_name}
              />
            </>
          )}
        </Group>
        {/* Its latest run beside what it scans, on a line that wraps rather than clip either. */}
        <Group gap={6}>
          <OutcomeBadge outcome={c.latest_outcome} />
          <Text
            c="var(--text-muted)"
            ff="text"
            size="xs"
            data-testid={`data-product-detail-dq-checks-${c.checker_table_id}`}
          >
            {t("dataProductCore.scans", { table: modelName(c.checked_table) })}
            {related &&
              ` · ${
                c.owner_product_id
                  ? t("dataProductCore.assignedTo", { product: c.owner_product_id })
                  : t("dataProductCore.unassigned")
              }`}
          </Text>
        </Group>
      </List.Item>
    );
  };

  const consumerRow = (tb: CoreTable) => {
    const field = rootOf(reader, tb);
    const graphql = field && reader ? graphqlExample(reader, field) : null;
    return (
      <List.Item key={`gql-${tb.id}`} ff="monospace">
        <Group gap={4} wrap="nowrap">
          <Text component="span" size="sm" ff="monospace">
            {modelName(tb)}
          </Text>
          {graphql && (
            <Tip text={t("dataProductsTab.graphqlQueryAria", { table: tb.table_name })}>
              <ActionIcon
                variant="subtle"
                size="sm"
                aria-label={t("dataProductsTab.graphqlQueryAria", { table: tb.table_name })}
                data-testid={`data-product-detail-graphql-query-${tb.id}`}
                onClick={() => navigate("/query", { state: { query: graphql, autoRun: true } })}
              >
                <Braces size={14} />
              </ActionIcon>
            </Tip>
          )}
          {sqlDrill(tb, `data-product-detail-sql-query-${tb.id}`)}
          <Tip text={t("dataProductsTab.jsonapiQueryAria", { table: tb.table_name })} named>
            <ActionIcon
              variant="subtle"
              size="sm"
              aria-label={t("dataProductsTab.jsonapiQueryAria", { table: tb.table_name })}
              data-testid={`data-product-detail-jsonapi-query-${tb.id}`}
              onClick={() =>
                navigate("/jsonapi", {
                  state: {
                    jsonapiUrl: `/data/jsonapi/${tb.domain_id}/${tb.table_name}`,
                    autoRun: true,
                  },
                })
              }
            >
              <FileJson size={14} />
            </ActionIcon>
          </Tip>
          {field && (
            <Tip text={t("dataProductsTab.openApiQueryAria", { table: tb.table_name })} named>
              <ActionIcon
                variant="subtle"
                size="sm"
                aria-label={t("dataProductsTab.openApiQueryAria", { table: tb.table_name })}
                data-testid={`data-product-detail-openapi-query-${tb.id}`}
                onClick={() =>
                  navigate("/openapi", {
                    state: { openApiUrl: `GET /data/rest/${field}`, autoRun: true },
                  })
                }
              >
                <Globe size={14} />
              </ActionIcon>
            </Tip>
          )}
          {field && (
            <Tip text={t("dataProductsTab.grpcQueryAria", { table: tb.table_name })} named>
              <ActionIcon
                variant="subtle"
                size="sm"
                aria-label={t("dataProductsTab.grpcQueryAria", { table: tb.table_name })}
                data-testid={`data-product-detail-grpc-query-${tb.id}`}
                onClick={() =>
                  navigate("/grpc", { state: { grpcMethod: grpcMethod(field), autoRun: true } })
                }
              >
                <Cable size={14} />
              </ActionIcon>
            </Tip>
          )}
        </Group>
      </List.Item>
    );
  };

  return (
    <Stack gap="sm" data-testid={`data-product-detail-${p.id}`}>
      <div style={{ columnWidth: "320px", columnGap: "var(--mantine-spacing-sm)" }}>
        <DetailCard
          section="details"
          testId={`data-product-card-details-${p.id}`}
          label={t("dataProductsTab.detail.field.details")}
        >
          <DetailFields testId={`data-product-read-${p.id}`} fields={fields} />
        </DetailCard>
        <DetailCard
          section="customProperties"
          testId={`data-product-card-customProperties-${p.id}`}
          label={t("dataProductsTab.detail.field.customProperties")}
        >
          {customPropertyEntries.length === 0 ? (
            <Empty text={t("dataProductsTab.detail.customPropertiesEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-custom-properties-${p.id}`}>
              {customPropertyEntries.map(([key, val]) => (
                <List.Item key={key} ff="monospace">
                  {key}
                  <Muted>= {String(val)}</Muted>
                </List.Item>
              ))}
            </List>
          )}
        </DetailCard>
        {core.tags !== undefined && (
          <DetailCard
            section="tags"
            testId={`data-product-card-tags-${p.id}`}
            label={t("dataProductCore.tags")}
          >
            {core.tags.length === 0 ? (
              <Empty text={t("dataProductCore.tagsEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-tags-${p.id}`}>
                {core.tags.map((tag) => (
                  <List.Item key={tag} ff="monospace">
                    {tag}
                  </List.Item>
                ))}
              </List>
            )}
          </DetailCard>
        )}
        {(outputTables !== undefined || core.commands !== undefined) && (
          <DetailCard
            section="outputPorts"
            testId={`data-product-card-outputPorts-${p.id}`}
            label={t("dataProductsTab.tablesLabel")}
            hint={t("dataProductsTab.detail.outputPortsHint")}
          >
            {/* REQ-2132: the product's promises, the checks it makes on its output ports. */}
            {dq && (
              <ClaimsCallout
                kind="promises"
                checks={dq.promises}
                testId={`data-product-detail-promises-${p.id}`}
              />
            )}
            {outputTables !== undefined && (
              <>
                {outputTables.length === 0 ? (
                  <Empty text={t("dataProductsTab.tablesEmpty")} />
                ) : (
                  <List size="sm" data-testid={`data-product-detail-tables-${p.id}`}>
                    {outputTables.map((tb) => (
                      <List.Item key={tb.id} ff="monospace">
                        {modelName(tb)}
                        {dq && (
                          <DqIcon
                            checks={dq.promises}
                            tableId={tb.id}
                            testId={`data-product-detail-port-dq-${tb.id}`}
                          />
                        )}
                        {tb.description && <Muted>— {tb.description}</Muted>}
                      </List.Item>
                    ))}
                  </List>
                )}
              </>
            )}
            {core.commands !== undefined && (
              <div
                style={{
                  marginTop: "var(--mantine-spacing-sm)",
                  marginLeft: "var(--mantine-spacing-md)",
                }}
              >
                <Text c="dimmed" fw={500} size="xs" tt="uppercase">
                  {t("dataProductsTab.commandsLabel")}
                </Text>
                {core.commands.length === 0 ? (
                  <Empty text={t("dataProductsTab.commandsEmpty")} />
                ) : (
                  <List size="sm" data-testid={`data-product-detail-commands-${p.id}`}>
                    {core.commands.map((fn) => (
                      <List.Item key={fn.name} ff="monospace">
                        {fn.name}
                        {fn.description && <Muted>— {fn.description}</Muted>}
                      </List.Item>
                    ))}
                  </List>
                )}
              </div>
            )}
          </DetailCard>
        )}
        {dq && (
          <DetailCard
            section="dataQuality"
            testId={`data-product-card-dataQuality-${p.id}`}
            label={t("dataProductsTab.detail.field.dataQuality")}
            hint={t("dataProductsTab.detail.dataQualityHint")}
            tourId="data-product-dq"
          >
            {shownDqRows.every((row) => dq[row].length === 0) &&
            (!owner || dq.unattached.length === 0) ? (
              <Empty text={t("dataProductsTab.detail.dataQualityEmpty")} />
            ) : (
              // REQ-2157: three expandable rows -- the product's expectations on its inputs, its
              // promises on its members, and the related checks others make on either.
              <div data-testid={`data-product-detail-dq-tables-${p.id}`}>
                <Collapsibles id="data-product.dq-rows">
                  {shownDqRows.map((row) => {
                    const status = dqStatus(dq[row]);
                    return (
                      <CollapsibleItem
                        key={row}
                        value={row}
                        testId={`data-product-detail-dq-${row}-${p.id}`}
                        controlTestId={`data-product-detail-dq-${row}-${p.id}-toggle`}
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
                          <Empty text={t("dataProductCore.dqRowEmpty")} />
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
                mt="xs"
                title={t("dataProductCore.orphan")}
                data-testid={`data-product-detail-dq-${p.id}-orphan-${c.checker_table_id}`}
              >
                {t("dataProductCore.orphanWarning", {
                  checker: modelName(c),
                  table: modelName(c.checked_table),
                })}
              </Alert>
            ))}
          </DetailCard>
        )}
        {profiled !== undefined && outputTables?.some((tb) => tb.profile_results !== undefined) && (
          <DetailCard
            section="profiles"
            testId={`data-product-card-profiles-${p.id}`}
            label={t("dataProductCore.profiles")}
            hint={t("dataProductCore.profilesHint")}
          >
            {profiled.length === 0 ? (
              <Empty text={t("dataProductCore.profilesEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-profiles-${p.id}`}>
                {profiled.map((tb) => (
                  <List.Item key={tb.id} ff="monospace">
                    {modelName(tb)}
                    <List size="xs" withPadding>
                      {(tb.profile_results ?? []).map((r) => (
                        <List.Item key={r.id} ff="monospace">
                          <Group gap={4} wrap="nowrap">
                            <Text component="span" size="xs" ff="monospace">
                              {r.table_name}
                            </Text>
                            {sqlDrill(r, `data-product-detail-profile-sql-query-${r.id}`)}
                          </Group>
                        </List.Item>
                      ))}
                    </List>
                  </List.Item>
                ))}
              </List>
            )}
          </DetailCard>
        )}
        {core.terms !== undefined && (
          <DetailCard
            section="relatedTerms"
            testId={`data-product-card-relatedTerms-${p.id}`}
            label={t("dataProductsTab.relatedTermsLabel")}
          >
            {core.terms.length === 0 ? (
              <Empty text={t("dataProductsTab.relatedTermsEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-related-terms-${p.id}`}>
                {core.terms.map((term) => (
                  <List.Item key={term.id} ff="monospace">
                    {term.name}
                    {term.is_abstract && (
                      <Muted>({t("dataProductsTab.relatedTermsConcept")})</Muted>
                    )}
                    {term.definition && <Muted>— {term.definition}</Muted>}
                  </List.Item>
                ))}
              </List>
            )}
          </DetailCard>
        )}
        {outputTables !== undefined && (
          <DetailCard
            section="consumerQueries"
            testId={`data-product-card-consumerQueries-${p.id}`}
            label={t("dataProductsTab.graphqlQueriesLabel")}
          >
            {outputTables.length === 0 && cypherRows.length === 0 ? (
              <Empty text={t("dataProductsTab.graphqlQueriesEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-graphql-queries-${p.id}`}>
                {outputTables.map(consumerRow)}
                {cypherRows.map(({ r, type }) => (
                  <List.Item key={`cy-${r.id}`} ff="monospace">
                    <Group gap={4} wrap="nowrap">
                      <Text component="span" size="sm" ff="monospace">
                        {type}
                      </Text>
                      <Tip text={t("dataProductsTab.cypherQueryAria", { type })} named>
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("dataProductsTab.cypherQueryAria", { type })}
                          data-testid={`data-product-detail-cypher-query-${r.id}`}
                          onClick={() =>
                            navigate("/graph", {
                              state: {
                                query: `MATCH ()-[r:${type}]->() RETURN r LIMIT 25`,
                                autoRun: true,
                              },
                            })
                          }
                        >
                          <Share2 size={14} />
                        </ActionIcon>
                      </Tip>
                    </Group>
                  </List.Item>
                ))}
              </List>
            )}
          </DetailCard>
        )}
        {related !== undefined && (
          <DetailCard
            section="relatedTables"
            testId={`data-product-card-relatedTables-${p.id}`}
            label={t("dataProductsTab.detail.field.relatedTables")}
            hint={t("dataProductsTab.detail.relatedTablesHint")}
          >
            {related.length === 0 ? (
              <Empty text={t("dataProductsTab.detail.relatedTablesEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-related-tables-${p.id}`}>
                {related.map((e) => (
                  <List.Item key={e.id} ff="monospace">
                    {/* The whole entry at most three lines; its ellipsis opens it in place. */}
                    <ClampedDetail
                      testId={`data-product-related-${p.id}-${e.target_product_id ?? e.target_table_id}`}
                    >
                      {relatedLabel(e)}
                      {e.target_product && (
                        <Muted>
                          ({t("dataProductCore.inProduct", { product: e.target_product.name })})
                        </Muted>
                      )}
                      <Text size="xs" c="var(--text-muted)">
                        {[
                          e.aliases,
                          e.path_count > 1
                            ? t("dataProductCore.paths", { count: e.path_count })
                            : null,
                        ]
                          .filter(Boolean)
                          .join(" · ")}
                      </Text>
                      {e.target_table?.description && (
                        <Text size="xs" c="var(--text-muted)">
                          {e.target_table.description}
                        </Text>
                      )}
                    </ClampedDetail>
                  </List.Item>
                ))}
              </List>
            )}
          </DetailCard>
        )}
        <DetailCard
          section="relationships"
          testId={`data-product-card-relationships-${p.id}`}
          label={t("dataProductsTab.detail.field.relationships")}
        >
          {internalRelationships.length === 0 ? (
            <Empty text={t("dataProductsTab.detail.relationshipsEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-relationships-${p.id}`}>
              {internalRelationships.map((r) => (
                <List.Item key={r.id} ff="monospace">
                  {/* GQL field name and Cypher relationship type are independent names for the
                      same edge — show both when set. */}
                  {joinAliasPair([r.graphql_field, r.cypher_type])}
                  <Muted>
                    {r.source.table_name}.{r.source_column}
                    {" → "}
                    {r.target_table?.table_name}
                    {r.target_column ? `.${r.target_column}` : ""} ({r.cardinality})
                  </Muted>
                </List.Item>
              ))}
            </List>
          )}
        </DetailCard>
        {/* REQ-2126, REQ-2132: the nearest physical inputs and the product's expectations of them,
            found on the product's lineage as its reader may read it (REQ-2156). */}
        {lineageGraph && owner && (
          <DetailCard
            section="inputPorts"
            testId={`data-product-card-inputPorts-${p.id}`}
            label={t("dataProductsTab.detail.field.inputPorts")}
            hint={t("dataProductCore.inputsHint")}
          >
            <ClaimsCallout
              kind="expectations"
              checks={lineageGraph.dq.expectations}
              testId={`data-product-detail-expectations-${p.id}`}
            />
            <InputsList
              inputs={lineageGraph.inputs}
              expectations={lineageGraph.dq.expectations}
              testId={`data-product-detail-inputs-${p.id}`}
            />
          </DetailCard>
        )}
        {/* Compact: the product, its members, commands and bound terms; the full graph opens
            from its expand button. */}
        <DetailCard
          section="graph"
          testId={`data-product-card-graph-${p.id}`}
          label={t("graph3d.panelTitle")}
        >
          <GraphPanel productId={p.id} productName={product.name} graph={graph} />
        </DetailCard>
      </div>
      {/* REQ-1640: the product's lineage DAG, full width below the cards, as the Lineage page draws
          it; the tallest panel on the page, so it starts closed. */}
      <div>
        <Collapsibles id="data-products.lineage">
          <CollapsibleItem
            value="lineage"
            defaultOpen={false}
            controlTestId={`data-product-detail-lineage-toggle-${p.id}`}
            title={
              <Group gap={4} align="baseline" component="span">
                <span>{t("dataProductsTab.detail.field.lineage")}</span>
                <Tip text={t("dataProductsTab.detail.lineageHint")}>
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
            {lineageLoading ? (
              <Text size="sm" c="var(--text-muted)">
                {t("dataProductsTab.detail.lineageLoading")}
              </Text>
            ) : lineageError ? (
              <Text size="sm" c="red">
                {lineageError}
              </Text>
            ) : !lineageGraph || lineageGraph.nodes.length === 0 ? (
              <Text size="sm" c="var(--text-muted)">
                {t("dataProductsTab.detail.lineageEmpty")}
              </Text>
            ) : (
              <div data-testid={`data-product-detail-lineage-${p.id}`}>
                <LineageDag
                  graph={lineageGraph}
                  height={720}
                  readable
                  relationLabel={relationLabel}
                  collapsedRelations={collapsed}
                  onToggleRelation={toggleRelation}
                  lanes={lineageMembers && { members: lineageMembers, title: laneTitle }}
                  onOpenModal={() => setLineageModalOpen(true)}
                  describeColumn={describeColumn}
                />
                <Modal
                  opened={lineageModalOpen}
                  onClose={() => setLineageModalOpen(false)}
                  size="90%"
                  title={t("dataProductsTab.detail.field.lineage")}
                  data-testid={`data-product-detail-lineage-modal-${p.id}`}
                  styles={{ body: { padding: "var(--mantine-spacing-xs)" } }}
                >
                  <LineageDag
                    graph={lineageGraph}
                    height="calc(90vh - 140px)"
                    relationLabel={relationLabel}
                    collapsedRelations={collapsed}
                    onToggleRelation={toggleRelation}
                    lanes={lineageMembers && { members: lineageMembers, title: laneTitle }}
                    describeColumn={describeColumn}
                  />
                </Modal>
              </div>
            )}
          </CollapsibleItem>
        </Collapsibles>
      </div>
      {canEdit && (
        <Group gap="xs" mt={4}>
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
          <Tip text={t("dataProductsTab.deleteDataProduct", { id: p.id })} named>
            <ActionIcon
              variant="subtle"
              color="red"
              aria-label={t("dataProductsTab.deleteDataProduct", { id: p.id })}
              data-testid={`data-product-detail-delete-${p.id}`}
              onClick={(e) => {
                e.stopPropagation();
                onDelete();
              }}
            >
              <Trash2 size={14} />
            </ActionIcon>
          </Tip>
        </Group>
      )}
    </Stack>
  );
}
