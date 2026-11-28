// Copyright (c) 2026 Kenneth Stott
// Canary: f8a956e5-5fc4-4a7a-beee-b23f491ed7f8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { ttlText } from "../../components/form/cacheTtl";
import { Alert, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import type { RefreshPolicySummary, RegisteredTable, Source } from "../../types/admin";
import { replicateLabel } from "../../components/admin/replicate";
import { READ_WAIT_NO_ROWS } from "./ReadWaitField";
import { useRegionChoices } from "../../hooks/useRegionQueries";
import { namingConventionLabel } from "../../hooks/useNamingConventions";
import { DetailFields, type DetailField } from "../../components/detail/DetailFields";
import { entityName } from "../../components/entityName";
import { EntityLink } from "../../components/EntityLink";
import { useSourceKind } from "../sources/sourceKind";
import { ReplicaBuildLine } from "./ReplicaBuildLine";
import {
  changeSignalChoices,
  isCheckerSource,
  isFilesSourceType,
  unset,
  yesNo,
} from "./tableSectionFields";
import {
  deltaFields,
  historyFields,
  liveFields,
  mvStoreFields,
  offPeakFields,
  pagingFields,
  snapshotFields,
} from "./loadReadFields";
import { viewMetricsFields } from "./queryReadFields";
import { QueryLinkRow } from "../../components/relationships/RelationshipQueryLinks";
import { useRelationshipSurfaces } from "../../components/relationships/useRelationshipSurfaces";
import { tableAggregateLinks, tableQueryLinks } from "../../components/relationships/tableQueries";
import { checkersField, uniquesField, useContractFields } from "./qualityReadFields";
import { IdentityParameters } from "./IdentityParameters";

// A table's sections read-only: the read view shows them, and so does the edit form to a caller
// who may change only how columns are hidden. Each lists the fields its edit tab has, in the same
// order and under the same labels; a value not set says what applies instead.

/** A table's effective refresh and serving policy, as the server derives it. */
/** What a read does with nothing landed, as the read view words it; null when the request timeout
 * applies, the default a read view leaves unsaid. */
function readWaitText(t: TFunction, readWait: number | null): string | null {
  if (readWait === null) return null;
  if (readWait === READ_WAIT_NO_ROWS) return t("landingScope.readWaitNoRows");
  return t("landingScope.readWaitUpTo", { seconds: readWait });
}

export function RefreshPolicyAlert({ policy }: { policy: RefreshPolicySummary }) {
  const { t } = useTranslation();
  return (
    <Alert
      // REQ-1143: `serving` drives the color; a non-null `warning` flags a setting that has no
      // effect on this engine or an accidental frozen table.
      color={
        policy.warning
          ? "yellow"
          : policy.serving === "live"
            ? "blue"
            : policy.serving === "scheduled"
              ? "teal"
              : policy.serving === "cache"
                ? "gray"
                : "orange"
      }
      title={t("tableEditForm.refreshPolicyTitle")}
      style={{ marginBottom: "0.75rem" }}
      data-testid="refresh-policy-summary"
    >
      <Text size="sm">{policy.text}</Text>
      {policy.warning && (
        <Text size="sm" c="yellow.8" mt={4} fw={500}>
          ⚠ {policy.warning}
        </Text>
      )}
    </Alert>
  );
}

const SOURCE_QUERY_LABEL: Record<string, string> = {
  sparql: "registerTableForm.sparqlQueryLabel",
  neo4j: "registerTableForm.neo4jCypherLabel",
  mongodb: "registerTableForm.pipelineQueryLabel",
  cosmosdb: "registerTableForm.cosmosQueryLabel",
  elasticsearch: "registerTableForm.esSearchLabel",
  qdrant: "registerTableForm.qdrantSearchLabel",
};

export function IdentityRead({
  table,
  source,
  actingRole,
}: {
  table: RegisteredTable;
  source?: Source;
  /** The acting roles, whose names the table's query links use; absent where none are shown. */
  actingRole?: string | null;
}) {
  const { t } = useTranslation();
  const regions = useRegionChoices().regions;
  // REQ-2252: one link per surface the table is exposed on to the acting role, each opening that
  // surface's Explore editor with a query that reads it, run at once; drawn as a relationship's
  // detail panel draws its links (amended 2026-11-22).
  const surfaces = useRelationshipSurfaces(actingRole ?? null);
  const queryLinks = actingRole ? tableQueryLinks(table, surfaces) : [];
  const naming = table.gqlNamingConvention
    ? namingConventionLabel(t, table.gqlNamingConvention)
    : t("tablesPage.inheritSource");
  const fields: DetailField[] = [
    { label: t("tableSections.source"), value: entityName("source", table.sourceId) },
    { label: t("tableSections.schema"), value: table.schemaName },
    { label: t("tableSections.table"), value: table.tableName },
    { label: t("tableSections.domain"), value: entityName("domain", table.domainId) },
    {
      label: t("tableEditForm.sqlAliasLabel"),
      value: table.alias ?? null,
    },
    { label: t("tableEditForm.namingConventionLabel"), value: naming },
    {
      label: t("tableEditForm.descriptionLabel"),
      value: table.description || null,
      testId: "table-identity-description",
    },
    ...(regions.length > 0
      ? [
          {
            label: t("regionSelect.label"),
            value: table.region ?? unset(t("regionSelect.none")),
            testId: "table-identity-region",
          },
        ]
      : []),
    // REQ-1923: a query-defined table's source query, in its source's own query language
    // (SPARQL, Cypher, a native pipeline...), read-only here; edited with the table-edit right.
    // It is never shown as the table's Cypher on the query tab.
    ...(source && table.queryTemplate && SOURCE_QUERY_LABEL[source.type.toLowerCase()]
      ? [
          {
            label: t(SOURCE_QUERY_LABEL[source.type.toLowerCase()]),
            value: <code className="detail-code">{table.queryTemplate}</code>,
            testId: "table-identity-sparql-query",
          },
        ]
      : []),
    ...(table.apiEndpoint
      ? [{ label: t("tableEditForm.apiEndpointLabel"), value: <code>{table.apiEndpoint}</code> }]
      : []),
    // Its source's document could not be read: no endpoint, and why.
    ...(table.apiEndpointError
      ? [
          {
            label: t("tableEditForm.apiEndpointLabel"),
            value: (
              <Text size="sm" c="var(--warning, #d19a00)">
                ⚠ {table.apiEndpointError}
              </Text>
            ),
            testId: "table-api-endpoint-error",
          },
        ]
      : []),
    ...(source && isFilesSourceType(source.type.toLowerCase())
      ? [
          {
            label: t("registerTableForm.fileGlobLabel"),
            value: table.fileGlob ? <code>{table.fileGlob}</code> : null,
          },
          {
            label: t("registerTableForm.sourceFileColumnLabel"),
            value: table.sourceFileColumn ? <code>{table.sourceFileColumn}</code> : null,
          },
        ]
      : []),
    // REQ-1954: where a ServiceNow table's inserts go: its import set, else the table itself.
    ...(source && source.type.toLowerCase() === "servicenow" && table.viewSql == null
      ? [
          {
            label: t("registerTableForm.importSetLabel"),
            value: table.importSet ? (
              <code>{table.importSet}</code>
            ) : (
              unset(t("registerTableForm.importSetNone"))
            ),
            testId: "table-identity-import-set",
          },
        ]
      : []),
  ];
  return (
    <>
      <QueryLinkRow
        links={queryLinks}
        labelOf={(surface) => t(`relationshipRow.queryLinks.${surface}`, { name: table.tableName })}
        heading={t("relationshipRow.queryLinks.heading")}
        testIdPrefix="table-identity"
      />
      {/* REQ-1986: a parameterized table's parameters, under the links whose queries carry them. */}
      <IdentityParameters columns={table.columns} />
      <DetailFields fields={fields} testId="table-identity" />
    </>
  );
}

export function LoadRead({ table, source }: { table: RegisteredTable; source?: Source }) {
  const { t } = useTranslation();
  const isView = table.viewSql != null;
  const signal = table.changeSignal ?? "";
  const probing = signal === "probe" || signal === "ttl_probe";
  const loadProtected = table.loadProtected ?? source?.loadProtected ?? false;
  const liveOnly = useSourceKind(source?.type ?? "")?.live_only === true;
  const fields: DetailField[] = [
    {
      label: t("tableEditForm.changeSignalLabel"),
      value: changeSignalChoices(t).find((c) => c.value === signal)?.label ?? signal,
    },
    ...(probing
      ? [
          {
            label: t("tableEditForm.probeTypeLabel"),
            value: table.probeType ?? t("tableEditForm.probeTypeAuto"),
          },
          {
            label: t("tableEditForm.freshnessProbeLabel"),
            value: table.probeQuery ? <code>{table.probeQuery}</code> : null,
          },
        ]
      : []),
    ...(isView
      ? [
          { label: t("tableEditForm.materializedViewLabel"), value: yesNo(t, table.materialize) },
          ...(table.materialize
            ? [
                {
                  label: t("tableEditForm.refreshIntervalLabel"),
                  value: String(table.mvRefreshInterval),
                },
                {
                  label: t("tableEditForm.nrtDebounceLabel"),
                  value: `${table.mvDebounceQuiet} / ${table.mvDebounceMaxDelay}`,
                },
              ]
            : []),
          ...mvStoreFields(t, table),
          ...snapshotFields(t, table),
        ]
      : [
          {
            label: t("tableEditForm.cacheTtlLabel"),
            value:
              table.cacheTtl != null
                ? ttlText(table.cacheTtl, t)
                : unset(
                    source?.cacheTtl != null
                      ? t("tableSections.fromSource", { value: ttlText(source.cacheTtl, t) })
                      : t("tableReadView.none"),
                  ),
          },
          {
            label: t("replicateSelect.label"),
            value:
              table.loadProtected === true
                ? t("replicateSelect.scheduled")
                : replicateLabel(t, table.replicate),
          },
          {
            label: t("tableEditForm.roleTtlLabel"),
            value:
              table.roleTtl.length > 0
                ? table.roleTtl.map((r) => `${r.role}: ${r.ttl}`).join(", ")
                : null,
          },
          ...pagingFields(t, table),
          {
            label: t("landingScope.readWaitLabel"),
            value: readWaitText(t, table.readWait ?? null),
          },
          {
            label: t("landingScope.readFilter"),
            value: table.landingFilter ? <code>{table.landingFilter}</code> : null,
          },
          ...(table.arguments && Object.keys(table.arguments).length > 0
            ? [
                {
                  label: t("landingScope.readArguments"),
                  value: <code>{JSON.stringify(table.arguments)}</code>,
                },
              ]
            : []),
          // A window the table does not set is its source's: said as that, not as none.
          ...(loadProtected
            ? table.offPeakWindow
              ? offPeakFields(t, table)
              : [
                  {
                    label: t("tableEditForm.offPeakWindowLabel"),
                    value: unset(t("tableSections.fromSourceAsIs")),
                  },
                  {
                    label: t("tableEditForm.offPeakTzLabel"),
                    value: table.offPeakTz ?? unset(t("tableSections.fromSourceAsIs")),
                  },
                ]
            : []),
          // REQ-2109: the columns the table keeps its versions by, when it declares them.
          ...historyFields(t, table),
        ]),
    { label: t("tableEditForm.delta.enable"), value: yesNo(t, table.delta != null) },
    ...deltaFields(t, table),
    { label: t("liveDeliveryFieldset.enableLabel"), value: yesNo(t, table.live != null) },
    ...(table.live
      ? [
          {
            label: t("liveDeliveryFieldset.pushDebounceLabel"),
            value: `${table.pushDebounceQuiet} / ${table.pushDebounceMaxDelay}`,
          },
        ]
      : []),
    ...liveFields(t, table),
  ];
  return (
    <>
      {table.refreshPolicySummary && <RefreshPolicyAlert policy={table.refreshPolicySummary} />}
      <DetailFields fields={fields} testId="table-load" />
      {/* Where the table's replica stands, its indexes with it: as the section's form shows it. */}
      {!isView && !liveOnly && (
        <ReplicaBuildLine
          sourceId={table.sourceId}
          schemaName={table.schemaName}
          tableName={table.tableName}
        />
      )}
    </>
  );
}

export function QueryRead({
  table,
  actingRole,
}: {
  table: RegisteredTable;
  /** The acting roles, whose names the table's aggregate links use; absent where none are shown. */
  actingRole?: string | null;
}) {
  const { t } = useTranslation();
  // REQ-2252 (amended 2026-11-22): with aggregates enabled, a link per surface that serves the
  // table's aggregates to the acting role, each opening that surface's Explore editor with an
  // aggregate query (a group-by with group-by enabled), run at once.
  const surfaces = useRelationshipSurfaces(
    actingRole && table.enableAggregates ? actingRole : null,
  );
  const aggregateLinks =
    actingRole && table.enableAggregates ? tableAggregateLinks(table, surfaces) : [];
  const fields: DetailField[] = [
    { label: t("tableEditForm.enableAggregatesLabel"), value: yesNo(t, table.enableAggregates) },
    { label: t("tableEditForm.enableGroupByLabel"), value: yesNo(t, table.enableGroupBy) },
    {
      label: t("tableReadView.writable"),
      testId: "table-read-view-writable",
      value:
        table.writeOps.length > 0
          ? table.writeOps.map((op) => op.toUpperCase()).join(", ")
          : t("tableReadView.noWrites"),
    },
    ...(table.watermarkColumn
      ? [
          {
            label: t("registerTableForm.watermarkLabel"),
            value: <code>{table.watermarkColumn}</code>,
          },
        ]
      : []),
    ...(table.viewSql
      ? [
          {
            label: t("tableReadView.viewSql"),
            value: <code className="detail-code">{table.viewSql}</code>,
          },
        ]
      : []),
    ...viewMetricsFields(t, table),
  ];
  return (
    <>
      <QueryLinkRow
        links={aggregateLinks}
        labelOf={(surface) =>
          t(`relationshipRow.queryLinks.aggregate.${surface}`, { name: table.tableName })
        }
        heading={t("relationshipRow.queryLinks.heading")}
        testIdPrefix="table-query-aggregate"
      />
      <DetailFields fields={fields} testId="table-query" />
    </>
  );
}

export function QualityRead({ table, source }: { table: RegisteredTable; source?: Source }) {
  const { t } = useTranslation();
  const checker = isCheckerSource(source);
  const contract = useContractFields(
    t,
    checker ? (source?.type ?? "").toLowerCase() : null,
    table.dqContract,
  );
  const fields: DetailField[] = [
    ...contract,
    ...(checker || source?.type === "data_profiler"
      ? []
      : [
          {
            label: t("profilerPanel.title"),
            value: table.profilerSourceId ? (
              <EntityLink kind="source" id={table.profilerSourceId} testId="quality-profiler-link">
                {table.profilerSourceId}
              </EntityLink>
            ) : null,
          },
        ]),
    checkersField(t, table.qualityCheckers),
    uniquesField(t, table.uniqueConstraints),
  ];
  return <DetailFields fields={fields} testId="table-quality" />;
}
