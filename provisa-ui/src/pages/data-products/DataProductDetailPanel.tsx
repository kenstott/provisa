// Copyright (c) 2026 Kenneth Stott
// Canary: 0f62c0c5-a9e2-4f0c-812f-d84dd160d2c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Collapse, Group, List, Stack, Table, Text, Tooltip } from "@mantine/core";
import {
  Braces,
  Cable,
  ChevronDown,
  ChevronRight,
  FileJson,
  Globe,
  ListChecks,
  Pencil,
  Share2,
  Table as TableIcon,
  Trash2,
} from "lucide-react";
import type { DataProduct, RegisteredTable, Relationship, Source } from "../../types/admin";
import { DqRulesModal } from "./DqRulesModal";
import { LineageDag } from "../../components/lineage/LineageDag";
import type { LineageGraphData } from "../../api/lineage";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import type { TrackedFunction } from "../../api/actions";
import type { RelatedGlossaryTerm } from "../../api/glossary";
import { cypherRelType } from "../../naming";
import { normalizeDomain } from "../sql/sqlHelpers";

interface RelatedTable {
  table: RegisteredTable;
  relationship: Relationship;
}

// ODPS inputPorts: a derived (not stored) 1-hop input(s) -> transform -> output projection over
// this product's lineage, computed by the parent page's inputPortsFor() from the raw lineage graph.
export interface InputPortRow {
  output: string;
  transform: string;
  inputs: string[];
}

// REQ-1634: GraphQL Queries panel — one example query per member table, using the
// server-derived root field name (graphqlFieldName) so the domain-wide uniqueness algorithm in
// provisa.compiler.naming.generate_name isn't reimplemented client-side.
function buildGraphqlExampleQuery(table: RegisteredTable): string {
  const fields = table.columns.map((c) => c.computedGqlAlias).join(" ");
  return `{ ${table.graphqlFieldName} { ${fields} } }`;
}

// REQ-1634: mirrors nativeParams.ts's tableRef convention (normalizeDomain + quoted identifiers).
function buildSelectAllSql(table: RegisteredTable): string {
  return `select * from "${normalizeDomain(table.domainId)}"."${table.alias || table.tableName}"`;
}

// ODPS: the JSON:API, REST/OpenAPI and gRPC surfaces are just other expressions of the same
// output port GraphQL already exposes, so every one of them can be derived from the same
// server-derived graphqlFieldName rather than re-deriving table identity per protocol.

// Mirrors JsonApiPage's own parsedNav regex (/^\/data\/jsonapi\/([^/]+)\/([^?]+)/).
function buildJsonApiUrl(table: RegisteredTable): string {
  return `/data/jsonapi/${table.domainId}/${table.tableName}`;
}

// Mirrors provisa/api/rest/generator.py: "For each root query field, generates GET /data/rest/{table}."
function buildOpenApiUrl(table: RegisteredTable): string {
  return `GET /data/rest/${table.graphqlFieldName}`;
}

// Mirrors provisa.compiler.naming.to_type_name + provisa.grpc.proto_gen._to_proto_type_name — both
// pure functions of the field name alone (no uniqueness resolution to reimplement, unlike
// generate_name). sa__userByName -> Sa__UserByName -> SaUserByName.
function buildGrpcMethod(table: RegisteredTable): string {
  const fieldName = table.graphqlFieldName ?? "";
  if (!fieldName) return "";
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
  return names.filter(Boolean).join("/\u200B");
}

interface DataProductDetailPanelProps {
  p: DataProduct;
  tables: RegisteredTable[];
  // REQ-1443 clause 10: the checker type a scan's rules are parsed with is the type of the source
  // the checker table sits on (soda | great_expectations) — RegisteredTable carries only sourceId.
  sources: Source[];
  // REQ-1634: member commands (Function rows whose product_id points at this data product).
  functions: TrackedFunction[];
  // REQ-1634: terms tied to any member-table column, plus the full transitive closure of
  // abstract terms reachable from those via edges — fetched server-side by the parent page.
  relatedTerms: RelatedGlossaryTerm[];
  relatedTermsLoading: boolean;
  relatedTables: RelatedTable[];
  // Relationships where both endpoints are member tables — connections entirely within the
  // product, distinct from relatedTables' one-hop-outside view. Relationship has no description
  // field, so its label is synthesized from the endpoint columns and cardinality.
  internalRelationships: Relationship[];
  // REQ-1634: gates Edit/Delete — a data_product_read-only caller sees the facts but not the
  // controls, same pattern as GlossaryTab's canEdit.
  canEdit: boolean;
  // REQ-1640: lineage is gated on view_governance, independent of the data_product_read/rw
  // pair — hidden entirely (not just disabled) when the caller lacks it.
  canSeeLineage: boolean;
  lineageLoading: boolean;
  lineageError: string;
  // REQ-1640: scoped subgraph (member tables, the published endpoint, plus their full upstream
  // derivation chain) — rendered with the same LineageDag component the Lineage page uses, not
  // a separate table-list view.
  lineageGraph: LineageGraphData | null;
  // REQ-1660 (ODPS inputPorts): derived, not stored — computed by the parent page from the same
  // lineageGraph passed above.
  inputPorts: InputPortRow[];
  onEdit: () => void;
  onDelete: () => void;
}

// Row-click detail view for a data product — same detail-then-edit pattern as
// MetricDetailPanel (REQ-1323): the read-only facts with Edit/Delete inside.
export function DataProductDetailPanel({
  p,
  tables,
  sources,
  functions,
  relatedTerms,
  relatedTermsLoading,
  relatedTables,
  internalRelationships,
  canEdit,
  canSeeLineage,
  lineageLoading,
  lineageError,
  lineageGraph,
  inputPorts,
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
  const toggleRelation = (relation: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (!next.delete(relation)) next.add(relation);
      return next;
    });
  // The DAG is the tallest panel on this page by far — start it closed so it doesn't dominate
  // the masonry layout before the user has asked to see it.
  const [lineageOpen, setLineageOpen] = useState(false);
  // REQ-1443 clause 10: the checker table whose rules modal is open, by table id.
  const [rulesFor, setRulesFor] = useState<number | null>(null);

  // REQ-1634: Cypher click-through only for relationships that resolve to a usable type and
  // aren't opted out — mirrors GraphPage's handleRelClick input contract.
  const cypherRows = internalRelationships
    .map((r) => ({ r, type: cypherRelType(r) }))
    .filter((row): row is { r: Relationship; type: string } => !!row.type && !row.r.disableCypher);

  const customPropertyEntries = Object.entries(p.customProperties ?? {});

  // REQ-1443 clause 10: a checker table (one carrying a contract) is a member only because the
  // table it scans is — its rows are the product's quality evidence, not part of its published
  // surface, so it is listed under Data Quality rather than among the output ports.
  const outputTables = tables.filter((tb) => tb.dqContract == null);
  const dqTables = tables.filter((tb) => tb.dqContract != null);

  // Masonry panel grid: a CSS multi-column layout packs each section into whichever column has
  // room next, instead of flex-wrap's row-by-row layout (which pads every panel in a row up to
  // the row's tallest neighbor, leaving the shorter panels full of unused whitespace).
  const panelStyle: React.CSSProperties = {
    breakInside: "avoid",
    minWidth: 0,
    marginBottom: "var(--mantine-spacing-sm)",
    border: "1px solid var(--mantine-color-default-border)",
    borderRadius: "var(--mantine-radius-sm)",
    padding: "var(--mantine-spacing-sm)",
  };
  const widePanelStyle: React.CSSProperties = panelStyle;

  const rows: [string, React.ReactNode][] = [
    ["domain", p.domainId],
    ["name", p.name],
    [
      "team",
      <Stack key="team" gap={0}>
        <Text size="sm">{p.teamRole || "—"}</Text>
        {p.teamRole && <OwnerResolutionInline refs={[p.teamRole]} />}
      </Stack>,
    ],
    [
      "owner",
      <Stack key="owner" gap={0}>
        <Text size="sm">{p.ownerRole || "—"}</Text>
        {p.ownerRole && <OwnerResolutionInline refs={[p.ownerRole]} />}
      </Stack>,
    ],
    ["purpose", p.purpose || "—"],
    ["limitations", p.limitations || "—"],
    ["usage", p.usage || "—"],
    ["version", p.version || "—"],
    ["status", p.status || "—"],
    ["sla", p.sla || "—"],
    ["support", p.support || "—"],
  ];

  return (
    <Stack gap="sm" data-testid={`data-product-detail-${p.id}`}>
      <div
        style={{
          columnWidth: "320px",
          columnGap: "var(--mantine-spacing-sm)",
        }}
      >
        <dl
          style={{
            ...panelStyle,
            display: "grid",
            gridTemplateColumns: "max-content 1fr",
            gap: "0.25rem 1rem",
            margin: 0,
            color: "var(--text)",
          }}
        >
          {rows.map(([k, v]) => (
            <React.Fragment key={k}>
              <Text component="dt" c="dimmed" fw={500} size="sm">
                {t(`dataProductsTab.detail.field.${k}`)}
              </Text>
              <Text component="dd" m={0} size="sm">
                {v}
              </Text>
            </React.Fragment>
          ))}
        </dl>
        <div style={panelStyle}>
          <Text c="dimmed" fw={500} size="sm">
            {t("dataProductsTab.detail.field.customProperties")}
          </Text>
          {customPropertyEntries.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.detail.customPropertiesEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-custom-properties-${p.id}`}>
              {customPropertyEntries.map(([key, val]) => (
                <List.Item key={key} ff="monospace">
                  {key}
                  <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                    {" "}
                    = {String(val)}
                  </Text>
                </List.Item>
              ))}
            </List>
          )}
        </div>
        <div style={panelStyle}>
          <Group gap={4} align="baseline">
            <Text c="dimmed" fw={500} size="sm">
              {t("dataProductsTab.detail.field.outputPorts")}
            </Text>
            <Tooltip label={t("dataProductsTab.detail.outputPortsHint")}>
              <Text size="xs" c="var(--text-muted)" style={{ cursor: "help" }}>
                ⓘ
              </Text>
            </Tooltip>
          </Group>
          {outputTables.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.tablesEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-tables-${p.id}`}>
              {outputTables.map((tb) => (
                <List.Item key={tb.id} ff="monospace">
                  {tb.domainId}.{tb.tableName}
                  {tb.description && (
                    <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                      {" "}
                      — {tb.description}
                    </Text>
                  )}
                </List.Item>
              ))}
            </List>
          )}
          <div
            style={{
              marginTop: "var(--mantine-spacing-sm)",
              marginLeft: "var(--mantine-spacing-md)",
            }}
          >
            <Text c="dimmed" fw={500} size="xs" tt="uppercase">
              {t("dataProductsTab.commandsLabel")}
            </Text>
            {functions.length === 0 ? (
              <Text size="sm" c="var(--text-muted)">
                {t("dataProductsTab.commandsEmpty")}
              </Text>
            ) : (
              <List size="sm" data-testid={`data-product-detail-commands-${p.id}`}>
                {functions.map((fn) => (
                  <List.Item key={fn.name} ff="monospace">
                    {fn.name}
                    {fn.description && (
                      <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                        {" "}
                        — {fn.description}
                      </Text>
                    )}
                  </List.Item>
                ))}
              </List>
            )}
          </div>
        </div>
        <div style={panelStyle}>
          <Group gap={4} align="baseline">
            <Text c="dimmed" fw={500} size="sm">
              {t("dataProductsTab.detail.field.dataQuality")}
            </Text>
            <Tooltip label={t("dataProductsTab.detail.dataQualityHint")}>
              <Text size="xs" c="var(--text-muted)" style={{ cursor: "help" }}>
                ⓘ
              </Text>
            </Tooltip>
          </Group>
          {dqTables.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.detail.dataQualityEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-dq-tables-${p.id}`}>
              {dqTables.map((tb) => (
                <List.Item key={tb.id} ff="monospace">
                  <Group gap={4} wrap="nowrap">
                    <Text component="span" size="sm" ff="monospace">
                      {tb.domainId}.{tb.tableName}
                    </Text>
                    {/* The scan results are reached through SQL alone here — the other access
                        patterns belong to the output ports, not to the evidence about them. */}
                    <Tooltip label={t("dataProductsTab.sqlQueryAria", { table: tb.tableName })}>
                      <ActionIcon
                        variant="subtle"
                        size="sm"
                        aria-label={t("dataProductsTab.sqlQueryAria", { table: tb.tableName })}
                        data-testid={`data-product-detail-dq-sql-query-${tb.id}`}
                        onClick={() =>
                          navigate("/sql", { state: { sql: buildSelectAllSql(tb), autoRun: true } })
                        }
                      >
                        <TableIcon size={14} />
                      </ActionIcon>
                    </Tooltip>
                    {/* The rules the scan enforces, read through the checker's own parser. The
                        checker type comes from the table's source; sources load on their own
                        query, so the control waits for that answer rather than guessing a type. */}
                    {(() => {
                      const source = sources.find((s) => s.id === tb.sourceId);
                      return (
                        <>
                          <Tooltip label={t("dataProductsTab.detail.dqRulesAria", { table: tb.tableName })}>
                            <ActionIcon
                              variant="subtle"
                              size="sm"
                              disabled={source === undefined}
                              aria-label={t("dataProductsTab.detail.dqRulesAria", { table: tb.tableName })}
                              data-testid={`data-product-detail-dq-rules-${tb.id}`}
                              onClick={() => setRulesFor(tb.id)}
                            >
                              <ListChecks size={14} />
                            </ActionIcon>
                          </Tooltip>
                          {source !== undefined && tb.dqContract != null && (
                            <DqRulesModal
                              opened={rulesFor === tb.id}
                              onClose={() => setRulesFor(null)}
                              checker={source.type.toLowerCase()}
                              contractText={tb.dqContract}
                              tableName={tb.tableName}
                            />
                          )}
                        </>
                      );
                    })()}
                  </Group>
                  {tb.description && (
                    <Text c="var(--text-muted)" ff="text" size="xs">
                      {tb.description}
                    </Text>
                  )}
                </List.Item>
              ))}
            </List>
          )}
        </div>
        <div style={panelStyle}>
          <Text c="dimmed" fw={500} size="sm">
            {t("dataProductsTab.relatedTermsLabel")}
          </Text>
          {relatedTermsLoading ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.relatedTermsLoading")}
            </Text>
          ) : relatedTerms.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.relatedTermsEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-related-terms-${p.id}`}>
              {relatedTerms.map((term) => (
                <List.Item key={term.id} ff="monospace">
                  {term.name}
                  {term.is_abstract && (
                    <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                      {" "}
                      ({t("dataProductsTab.relatedTermsAbstract")})
                    </Text>
                  )}
                  {term.definition && (
                    <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                      {" "}
                      — {term.definition}
                    </Text>
                  )}
                </List.Item>
              ))}
            </List>
          )}
        </div>
        <div style={widePanelStyle}>
          <Text c="dimmed" fw={500} size="sm">
            {t("dataProductsTab.graphqlQueriesLabel")}
          </Text>
          {outputTables.length === 0 && cypherRows.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.graphqlQueriesEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-graphql-queries-${p.id}`}>
              {outputTables.map((tb) => (
                <List.Item key={`gql-${tb.id}`} ff="monospace">
                  <Group gap={4} wrap="nowrap">
                    <Text component="span" size="sm" ff="monospace">
                      {tb.domainId}.{tb.tableName}
                    </Text>
                    {tb.graphqlFieldName && (
                      <Tooltip
                        label={t("dataProductsTab.graphqlQueryAria", { table: tb.tableName })}
                      >
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("dataProductsTab.graphqlQueryAria", {
                            table: tb.tableName,
                          })}
                          data-testid={`data-product-detail-graphql-query-${tb.id}`}
                          onClick={() =>
                            navigate("/query", {
                              state: { query: buildGraphqlExampleQuery(tb), autoRun: true },
                            })
                          }
                        >
                          <Braces size={14} />
                        </ActionIcon>
                      </Tooltip>
                    )}
                    <Tooltip label={t("dataProductsTab.sqlQueryAria", { table: tb.tableName })}>
                      <ActionIcon
                        variant="subtle"
                        size="sm"
                        aria-label={t("dataProductsTab.sqlQueryAria", { table: tb.tableName })}
                        data-testid={`data-product-detail-sql-query-${tb.id}`}
                        onClick={() =>
                          navigate("/sql", { state: { sql: buildSelectAllSql(tb), autoRun: true } })
                        }
                      >
                        <TableIcon size={14} />
                      </ActionIcon>
                    </Tooltip>
                    <Tooltip label={t("dataProductsTab.jsonapiQueryAria", { table: tb.tableName })}>
                      <ActionIcon
                        variant="subtle"
                        size="sm"
                        aria-label={t("dataProductsTab.jsonapiQueryAria", { table: tb.tableName })}
                        data-testid={`data-product-detail-jsonapi-query-${tb.id}`}
                        onClick={() =>
                          navigate("/jsonapi", {
                            state: { jsonapiUrl: buildJsonApiUrl(tb), autoRun: true },
                          })
                        }
                      >
                        <FileJson size={14} />
                      </ActionIcon>
                    </Tooltip>
                    <Tooltip label={t("dataProductsTab.openApiQueryAria", { table: tb.tableName })}>
                      <ActionIcon
                        variant="subtle"
                        size="sm"
                        aria-label={t("dataProductsTab.openApiQueryAria", { table: tb.tableName })}
                        data-testid={`data-product-detail-openapi-query-${tb.id}`}
                        onClick={() =>
                          navigate("/openapi", {
                            state: { openApiUrl: buildOpenApiUrl(tb), autoRun: true },
                          })
                        }
                      >
                        <Globe size={14} />
                      </ActionIcon>
                    </Tooltip>
                    {tb.graphqlFieldName && (
                      <Tooltip label={t("dataProductsTab.grpcQueryAria", { table: tb.tableName })}>
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("dataProductsTab.grpcQueryAria", { table: tb.tableName })}
                          data-testid={`data-product-detail-grpc-query-${tb.id}`}
                          onClick={() =>
                            navigate("/grpc", {
                              state: { grpcMethod: buildGrpcMethod(tb), autoRun: true },
                            })
                          }
                        >
                          <Cable size={14} />
                        </ActionIcon>
                      </Tooltip>
                    )}
                  </Group>
                </List.Item>
              ))}
              {cypherRows.map(({ r, type }) => (
                <List.Item key={`cy-${r.id}`} ff="monospace">
                  <Group gap={4} wrap="nowrap">
                    <Text component="span" size="sm" ff="monospace">
                      {type}
                    </Text>
                    <Tooltip label={t("dataProductsTab.cypherQueryAria", { type })}>
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
                    </Tooltip>
                  </Group>
                </List.Item>
              ))}
            </List>
          )}
        </div>
        <div style={panelStyle}>
          <Group gap={4} align="baseline">
            <Text c="dimmed" fw={500} size="sm">
              {t("dataProductsTab.detail.field.relatedTables")}
            </Text>
            <Tooltip label={t("dataProductsTab.detail.relatedTablesHint")}>
              <Text size="xs" c="var(--text-muted)" style={{ cursor: "help" }}>
                ⓘ
              </Text>
            </Tooltip>
          </Group>
          {relatedTables.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.detail.relatedTablesEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-related-tables-${p.id}`}>
              {relatedTables.map(({ table, relationship: r }) => (
                <List.Item key={r.id} ff="monospace">
                  {table.domainId}.{table.tableName}
                  <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                    {" "}
                    {/* the relationship's GQL field name and Cypher relationship type are two
                        independent names for the same edge — show both when set, rather than
                        silently picking one, so this isn't mistaken for a column/table name.
                        alias is the stored Cypher type; computedCypherAlias is the server's
                        derived fallback when no alias was explicitly stored (mirrors _rel_from_row). */}
                    ({joinAliasPair([r.graphqlAlias, r.alias ?? r.computedCypherAlias]) || r.cardinality})
                  </Text>
                  {table.description ? (
                    <Text size="xs" c="var(--text-muted)">
                      {table.description}
                    </Text>
                  ) : null}
                </List.Item>
              ))}
            </List>
          )}
        </div>
        <div style={panelStyle}>
          <Text c="dimmed" fw={500} size="sm">
            {t("dataProductsTab.detail.field.relationships")}
          </Text>
          {internalRelationships.length === 0 ? (
            <Text size="sm" c="var(--text-muted)">
              {t("dataProductsTab.detail.relationshipsEmpty")}
            </Text>
          ) : (
            <List size="sm" data-testid={`data-product-detail-relationships-${p.id}`}>
              {internalRelationships.map((r) => (
                <List.Item key={r.id} ff="monospace">
                  {/* GQL field name and Cypher relationship type are independent names for the
                      same edge — show both when set (mirrors the Related Tables panel below). */}
                  {joinAliasPair([r.graphqlAlias, r.alias ?? r.computedCypherAlias]) ||
                    `${r.sourceTableName}.${r.sourceColumn}`}
                  <Text component="span" c="var(--text-muted)" ff="text" size="xs">
                    {" "}
                    {r.sourceTableName}.{r.sourceColumn}
                    {" → "}
                    {r.targetTableName}
                    {r.targetColumn ? `.${r.targetColumn}` : ""} ({r.cardinality})
                  </Text>
                </List.Item>
              ))}
            </List>
          )}
        </div>
        {canSeeLineage && (
          <div style={widePanelStyle}>
            <Group gap={4} align="baseline">
              <Text c="dimmed" fw={500} size="sm">
                {t("dataProductsTab.detail.field.inputPorts")}
              </Text>
              <Tooltip label={t("dataProductsTab.detail.inputPortsHint")}>
                <Text size="xs" c="var(--text-muted)" style={{ cursor: "help" }}>
                  ⓘ
                </Text>
              </Tooltip>
            </Group>
            {inputPorts.length === 0 ? (
              <Text size="sm" c="var(--text-muted)">
                {t("dataProductsTab.detail.inputPortsEmpty")}
              </Text>
            ) : (
              <Table
                striped
                withTableBorder
                data-testid={`data-product-detail-input-ports-${p.id}`}
              >
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>{t("dataProductsTab.detail.inputPortsColInputs")}</Table.Th>
                    <Table.Th>{t("dataProductsTab.detail.inputPortsColTransform")}</Table.Th>
                    <Table.Th>{t("dataProductsTab.detail.inputPortsColOutput")}</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {inputPorts.map((row, i) => (
                    <Table.Tr key={i}>
                      <Table.Td ff="monospace">{row.inputs.join(", ")}</Table.Td>
                      <Table.Td ff="monospace">{row.transform}</Table.Td>
                      <Table.Td ff="monospace">{row.output}</Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            )}
          </div>
        )}
        {canSeeLineage && (
          <div style={{ ...panelStyle, columnSpan: "all", marginTop: "var(--mantine-spacing-sm)" }}>
            <Group
              gap={4}
              align="baseline"
              style={{ cursor: "pointer" }}
              onClick={() => setLineageOpen((v) => !v)}
              data-testid={`data-product-detail-lineage-toggle-${p.id}`}
            >
              {lineageOpen ? (
                <ChevronDown size={14} color="var(--text-muted)" />
              ) : (
                <ChevronRight size={14} color="var(--text-muted)" />
              )}
              <Text c="dimmed" fw={500} size="sm">
                {t("dataProductsTab.detail.field.lineage")}
              </Text>
              <Tooltip label={t("dataProductsTab.detail.lineageHint")}>
                <Text
                  size="xs"
                  c="var(--text-muted)"
                  style={{ cursor: "help" }}
                  onClick={(e) => e.stopPropagation()}
                >
                  ⓘ
                </Text>
              </Tooltip>
            </Group>
            <Collapse in={lineageOpen}>
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
                    height={320}
                    collapsedRelations={collapsed}
                    onToggleRelation={toggleRelation}
                  />
                </div>
              )}
            </Collapse>
          </div>
        )}
      </div>
      {canEdit && (
        <Group gap="xs" mt={4}>
          <Tooltip label={t("dataProductsTab.editDataProduct", { id: p.id })}>
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
          </Tooltip>
          <Tooltip label={t("dataProductsTab.deleteDataProduct", { id: p.id })}>
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
          </Tooltip>
        </Group>
      )}
    </Stack>
  );
}
