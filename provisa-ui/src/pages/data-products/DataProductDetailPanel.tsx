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
import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import {
  ActionIcon,
  Alert,
  CopyButton,
  Group,
  List,
  Loader,
  Stack,
  Table,
  Text,
} from "@mantine/core";
import {
  Braces,
  Cable,
  Check,
  Copy,
  FileJson,
  Globe,
  ListChecks,
  Pencil,
  Share2,
  Table as TableIcon,
  Trash2,
} from "lucide-react";
import type { DataProduct, Relationship, Source } from "../../types/admin";
import { DqRulesModal } from "./DqRulesModal";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import { cypherRelType, tableSqlRef } from "../../naming";
import { Tip } from "../../components/Tip";
import { rootKey, type MetaReader } from "../../api/metaGraph";
import { KnowledgeGraphPanel } from "../../components/graph3d/KnowledgeGraphPanel";
import type { CoreEdge, CoreTable, ProductCore } from "./productCore";
import { ClaimsCallout, DqIcon, InputsList, PartnersAndOrphans } from "./dataProductDq";
import { checksIn } from "./dataProductClaims";

// ODPS inputPorts: a derived (not stored) 1-hop input(s) -> transform -> output projection over
// this product's lineage, computed by the parent page's inputPortsFor() from the raw lineage graph.
export interface InputPortRow {
  output: string;
  transform: string;
  inputs: string[];
}

interface TableRef {
  domain_id: string;
  table_name: string;
}

const sqlRefOf = (tb: TableRef) =>
  tableSqlRef({ domainId: tb.domain_id, tableName: tb.table_name });
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
  // REQ-1443 clause 10: the checker type a scan's rules are parsed with is the type of the source
  // the checker table sits on (soda | great_expectations).
  sources: Source[];
  // Relationships where both endpoints are member tables, for their Cypher click-through.
  internalRelationships: Relationship[];
  // REQ-1634: gates Edit/Delete.
  canEdit: boolean;
  // REQ-1640: lineage and input ports need view_governance; without it they are absent.
  canSeeLineage: boolean;
  // REQ-1660 (ODPS inputPorts): derived by the parent page from the federation lineage graph.
  inputPorts: InputPortRow[];
  onEdit: () => void;
  onDelete: () => void;
}

function SectionTitle({ label, hint }: { label: string; hint?: string }) {
  return (
    <Group gap={4} align="baseline">
      <Text c="dimmed" fw={500} size="sm">
        {label}
      </Text>
      {hint && (
        <Tip text={hint}>
          <Text size="xs" c="var(--text-muted)" style={{ cursor: "help" }}>
            ⓘ
          </Text>
        </Tip>
      )}
    </Group>
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

/** The panel's own reads, each copyable and opened in its explorer: the same answer either way. */
function MetaQueries({ core, testId }: { core: ProductCore; testId: string }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const rows: {
    key: string;
    label: string;
    text: string;
    open: () => void;
    icon: React.ReactNode;
  }[] = [
    {
      key: "graphql",
      label: "GraphQL",
      text: core.queries.graphql,
      icon: <Braces size={14} />,
      open: () =>
        navigate("/query", {
          state: { query: core.queries.graphql, autoRun: true },
        }),
    },
    {
      key: "sql",
      label: "SQL",
      text: core.queries.sql,
      icon: <TableIcon size={14} />,
      open: () => navigate("/sql", { state: { sql: core.queries.sql, autoRun: true } }),
    },
    {
      key: "jsonapi",
      label: "JSON:API",
      text: core.queries.jsonapi,
      icon: <FileJson size={14} />,
      open: () =>
        navigate("/jsonapi", { state: { jsonapiUrl: core.queries.jsonapi, autoRun: true } }),
    },
  ];
  return (
    <Stack gap={6} data-testid={testId}>
      {rows.map((r) => (
        <div key={r.key}>
          <Group gap={4} wrap="nowrap">
            <Text size="xs" fw={600}>
              {r.label}
            </Text>
            <CopyButton value={r.text}>
              {({ copied, copy }) => (
                <Tip text={copied ? t("dataProductCore.copied") : t("dataProductCore.copy")}>
                  <ActionIcon
                    variant="subtle"
                    size="sm"
                    onClick={copy}
                    aria-label={t("dataProductCore.copyQuery", { surface: r.label })}
                    data-testid={`${testId}-copy-${r.key}`}
                  >
                    {copied ? <Check size={14} /> : <Copy size={14} />}
                  </ActionIcon>
                </Tip>
              )}
            </CopyButton>
            <Tip text={t("dataProductCore.openQuery", { surface: r.label })} named>
              <ActionIcon
                variant="subtle"
                size="sm"
                onClick={r.open}
                aria-label={t("dataProductCore.openQuery", { surface: r.label })}
                data-testid={`${testId}-open-${r.key}`}
              >
                {r.icon}
              </ActionIcon>
            </Tip>
          </Group>
          <Text
            component="pre"
            size="xs"
            ff="monospace"
            // Bounded, so the nested read does not stand taller than every other section.
            style={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
              margin: 0,
              maxHeight: 220,
              overflowY: "auto",
            }}
            data-testid={`${testId}-${r.key}`}
          >
            {r.text}
          </Text>
        </div>
      ))}
    </Stack>
  );
}

function relatedLabel(e: CoreEdge): string {
  if (e.target_product) return e.target_product.name;
  if (e.target_table) return `${e.target_table.domain_id}.${e.target_table.table_name}`;
  return String(e.target_product_id ?? e.target_table_id);
}

// Row-click detail view for a data product — same detail-then-edit pattern as
// MetricDetailPanel (REQ-1323): the read-only facts with Edit/Delete inside.
export function DataProductDetailPanel({
  p,
  core,
  coreError,
  reader,
  sources,
  internalRelationships,
  canEdit,
  canSeeLineage,
  inputPorts,
  onEdit,
  onDelete,
}: DataProductDetailPanelProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // REQ-1443 clause 10: the checker table whose rules modal is open, by table id.
  const [rulesFor, setRulesFor] = useState<number | null>(null);

  // REQ-1634: Cypher click-through only for relationships that resolve to a usable type and
  // aren't opted out — mirrors GraphPage's handleRelClick input contract.
  const cypherRows = internalRelationships
    .map((r) => ({ r, type: cypherRelType(r) }))
    .filter((row): row is { r: Relationship; type: string } => !!row.type && !row.r.disableCypher);

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
  const related = core.edges?.filter((e) => e.kind === "relationship");
  const lineage = core.edges?.filter((e) => e.kind === "lineage");
  const upstream = lineage
    ?.filter((e) => e.direction === "upstream")
    .sort(
      (a, b) =>
        b.hops - a.hops || String(a.target_relation).localeCompare(String(b.target_relation)),
    );
  const downstream = lineage?.filter((e) => e.direction === "downstream");
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

  const consumerRow = (tb: CoreTable) => {
    const field = rootOf(reader, tb);
    const graphql = field && reader ? graphqlExample(reader, field) : null;
    return (
      <List.Item key={`gql-${tb.id}`} ff="monospace">
        <Group gap={4} wrap="nowrap">
          <Text component="span" size="sm" ff="monospace">
            {tb.domain_id}.{tb.table_name}
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
        <div style={panelStyle}>
          <DetailFields testId={`data-product-read-${p.id}`} fields={fields} />
        </div>
        <div style={panelStyle}>
          <SectionTitle label={t("dataProductsTab.detail.field.customProperties")} />
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
        </div>
        {core.tags !== undefined && (
          <div style={panelStyle}>
            <SectionTitle label={t("dataProductCore.tags")} />
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
          </div>
        )}
        {(outputTables !== undefined || core.commands !== undefined) && (
          <div style={panelStyle}>
            {outputTables !== undefined && (
              <>
                <SectionTitle
                  label={t("dataProductsTab.tablesLabel")}
                  hint={t("dataProductsTab.detail.outputPortsHint")}
                />
                {/* REQ-2132: the product's promises, the checks it makes on its output ports. */}
                {core.checks && (
                  <ClaimsCallout
                    kind="promises"
                    checks={checksIn(core.checks, "promise")}
                    testId={`data-product-detail-promises-${p.id}`}
                  />
                )}
                {outputTables.length === 0 ? (
                  <Empty text={t("dataProductsTab.tablesEmpty")} />
                ) : (
                  <List size="sm" data-testid={`data-product-detail-tables-${p.id}`}>
                    {outputTables.map((tb) => (
                      <List.Item key={tb.id} ff="monospace">
                        {tb.domain_id}.{tb.table_name}
                        {core.checks && (
                          <DqIcon
                            checks={checksIn(core.checks, "promise")}
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
          </div>
        )}
        {dqTables !== undefined && (
          <div style={panelStyle} data-tour="data-product-dq">
            <SectionTitle
              label={t("dataProductsTab.detail.field.dataQuality")}
              hint={t("dataProductsTab.detail.dataQualityHint")}
            />
            {dqTables.length === 0 ? (
              <Empty text={t("dataProductsTab.detail.dataQualityEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-dq-tables-${p.id}`}>
                {dqTables.map((tb) => {
                  // The checker type comes from the table's source; sources load on their own
                  // query, so the control waits for that answer rather than guessing a type.
                  const source = sources.find((s) => s.id === tb.source_id);
                  return (
                    <List.Item key={tb.id} ff="monospace">
                      {/* REQ-2132: the check's claim, why it exists, headlines it. */}
                      {tb.description && (
                        <Text
                          size="sm"
                          fw={600}
                          ff="text"
                          data-testid={`data-product-detail-dq-claim-${tb.id}`}
                        >
                          {tb.description}
                        </Text>
                      )}
                      <Group gap={4} wrap="nowrap">
                        <Text component="span" size="sm" ff="monospace">
                          {tb.domain_id}.{tb.table_name}
                        </Text>
                        {/* The scan results are reached through SQL alone here — the other
                            access patterns belong to the output ports, not to the evidence. */}
                        {sqlDrill(tb, `data-product-detail-dq-sql-query-${tb.id}`)}
                        <Tip
                          text={t("dataProductsTab.detail.dqRulesAria", { table: tb.table_name })}
                        >
                          <ActionIcon
                            variant="subtle"
                            size="sm"
                            disabled={source === undefined}
                            aria-label={t("dataProductsTab.detail.dqRulesAria", {
                              table: tb.table_name,
                            })}
                            data-testid={`data-product-detail-dq-rules-${tb.id}`}
                            onClick={() => setRulesFor(tb.id)}
                          >
                            <ListChecks size={14} />
                          </ActionIcon>
                        </Tip>
                        {source !== undefined && tb.dq_contract != null && (
                          <DqRulesModal
                            opened={rulesFor === tb.id}
                            onClose={() => setRulesFor(null)}
                            checker={source.type.toLowerCase()}
                            contractText={tb.dq_contract}
                            tableName={tb.table_name}
                          />
                        )}
                      </Group>
                      {tb.checks && (
                        <Text
                          c="var(--text-muted)"
                          ff="text"
                          size="xs"
                          data-testid={`data-product-detail-dq-checks-${tb.id}`}
                        >
                          {t("dataProductCore.scans", {
                            table: `${tb.checks.domain_id}.${tb.checks.table_name}`,
                          })}
                        </Text>
                      )}
                    </List.Item>
                  );
                })}
              </List>
            )}
            {core.checks && (
              <PartnersAndOrphans checks={core.checks} testId={`data-product-detail-dq-${p.id}`} />
            )}
          </div>
        )}
        {profiled !== undefined && outputTables?.some((tb) => tb.profile_results !== undefined) && (
          <div style={panelStyle}>
            <SectionTitle
              label={t("dataProductCore.profiles")}
              hint={t("dataProductCore.profilesHint")}
            />
            {profiled.length === 0 ? (
              <Empty text={t("dataProductCore.profilesEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-profiles-${p.id}`}>
                {profiled.map((tb) => (
                  <List.Item key={tb.id} ff="monospace">
                    {tb.domain_id}.{tb.table_name}
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
          </div>
        )}
        {core.terms !== undefined && (
          <div style={panelStyle}>
            <SectionTitle label={t("dataProductsTab.relatedTermsLabel")} />
            {core.terms.length === 0 ? (
              <Empty text={t("dataProductsTab.relatedTermsEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-related-terms-${p.id}`}>
                {core.terms.map((term) => (
                  <List.Item key={term.id} ff="monospace">
                    {term.name}
                    {term.is_abstract && (
                      <Muted>({t("dataProductsTab.relatedTermsAbstract")})</Muted>
                    )}
                    {term.definition && <Muted>— {term.definition}</Muted>}
                  </List.Item>
                ))}
              </List>
            )}
          </div>
        )}
        {outputTables !== undefined && (
          <div style={panelStyle}>
            <SectionTitle label={t("dataProductsTab.graphqlQueriesLabel")} />
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
          </div>
        )}
        {related !== undefined && (
          <div style={panelStyle}>
            <SectionTitle
              label={t("dataProductsTab.detail.field.relatedTables")}
              hint={t("dataProductsTab.detail.relatedTablesHint")}
            />
            {related.length === 0 ? (
              <Empty text={t("dataProductsTab.detail.relatedTablesEmpty")} />
            ) : (
              <List size="sm" data-testid={`data-product-detail-related-tables-${p.id}`}>
                {related.map((e) => (
                  <List.Item key={`${e.target_product_id}:${e.target_table_id}`} ff="monospace">
                    {relatedLabel(e)}
                    {e.target_product && <Muted>({t("dataProductCore.aProduct")})</Muted>}
                    <Text size="xs" c="var(--text-muted)">
                      {[
                        e.aliases,
                        e.direct
                          ? t("dataProductCore.direct")
                          : t("dataProductCore.hops", { count: e.hops }),
                        e.path_count > 1
                          ? t("dataProductCore.paths", { count: e.path_count })
                          : null,
                        e.supporting_tables
                          ? t("dataProductCore.through", { tables: e.supporting_tables })
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
                  </List.Item>
                ))}
              </List>
            )}
          </div>
        )}
        <div style={panelStyle}>
          <SectionTitle label={t("dataProductsTab.detail.field.relationships")} />
          {internalRelationships.length === 0 ? (
            <Empty text={t("dataProductsTab.detail.relationshipsEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-relationships-${p.id}`}>
              {internalRelationships.map((r) => (
                <List.Item key={r.id} ff="monospace">
                  {/* GQL field name and Cypher relationship type are independent names for the
                      same edge — show both when set. */}
                  {joinAliasPair([r.graphqlAlias, r.alias ?? r.computedCypherAlias]) ||
                    `${r.sourceTableName}.${r.sourceColumn}`}
                  <Muted>
                    {r.sourceTableName}.{r.sourceColumn}
                    {" → "}
                    {r.targetTableName}
                    {r.targetColumn ? `.${r.targetColumn}` : ""} ({r.cardinality})
                  </Muted>
                </List.Item>
              ))}
            </List>
          )}
        </div>
        {canSeeLineage && (
          <div style={panelStyle}>
            <SectionTitle
              label={t("dataProductsTab.detail.field.inputPorts")}
              hint={t("dataProductsTab.detail.inputPortsHint")}
            />
            {inputPorts.length === 0 ? (
              <Empty text={t("dataProductsTab.detail.inputPortsEmpty")} />
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
        {/* REQ-2126, REQ-2132: the nearest inputs and the product's expectations of them; lineage
            rows, so view_governance's like the Lineage section. */}
        {canSeeLineage && core.edges !== undefined && (
          <div style={panelStyle}>
            <SectionTitle
              label={t("dataProductCore.inputs")}
              hint={t("dataProductCore.inputsHint")}
            />
            {core.checks && (
              <ClaimsCallout
                kind="expectations"
                checks={checksIn(core.checks, "expectation")}
                testId={`data-product-detail-expectations-${p.id}`}
              />
            )}
            <InputsList
              edges={core.edges}
              checks={core.checks ?? []}
              testId={`data-product-detail-inputs-${p.id}`}
            />
          </div>
        )}
        {/* REQ-1640: data_product_edges answers lineage rows only to view_governance. */}
        {canSeeLineage && upstream !== undefined && downstream !== undefined && (
          <div style={panelStyle}>
            <SectionTitle
              label={t("dataProductsTab.detail.field.lineage")}
              hint={t("dataProductsTab.detail.lineageHint")}
            />
            {upstream.length + downstream.length === 0 ? (
              <Empty text={t("dataProductsTab.detail.lineageEmpty")} />
            ) : (
              <Stack gap={4} data-testid={`data-product-detail-lineage-${p.id}`}>
                {(
                  [
                    ["upstream", upstream],
                    ["downstream", downstream],
                  ] as const
                ).map(
                  ([dir, rows]) =>
                    rows.length > 0 && (
                      <div key={dir}>
                        <Text size="xs" fw={600} tt="uppercase" c="dimmed">
                          {t(`dataProductCore.${dir}`)}
                        </Text>
                        <List size="sm" data-testid={`data-product-detail-lineage-${dir}-${p.id}`}>
                          {rows.map((e) => (
                            <List.Item key={String(e.target_relation)} ff="monospace">
                              {e.target_relation}
                              <Muted>
                                {[
                                  dir === "upstream"
                                    ? t("dataProductCore.hops", { count: e.hops })
                                    : null,
                                  e.vias ? t("dataProductCore.via", { vias: e.vias }) : null,
                                ]
                                  .filter(Boolean)
                                  .join(" · ")}
                              </Muted>
                            </List.Item>
                          ))}
                        </List>
                      </div>
                    ),
                )}
              </Stack>
            )}
          </div>
        )}
        <div style={panelStyle}>
          <KnowledgeGraphPanel productId={p.id} productName={product.name} />
        </div>
        <div style={panelStyle}>
          <SectionTitle
            label={t("dataProductCore.metaQueries")}
            hint={t("dataProductCore.metaQueriesHint")}
          />
          <MetaQueries core={core} testId={`data-product-detail-meta-queries-${p.id}`} />
        </div>
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
