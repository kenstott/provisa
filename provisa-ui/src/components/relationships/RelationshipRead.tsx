// Copyright (c) 2026 Kenneth Stott
// Canary: 34a1003a-0cfc-40cf-b0c1-089cc2799b6d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import type { Relationship } from "../../types/admin";
import { cypherRelType, domainToSqlName } from "../../naming";
import { DetailFields } from "../detail/DetailFields";

/** A heading over a group of the read view's fields, as the form heads its own. */
function ReadHeading({ children }: { children: string }) {
  return (
    <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase" mb="xs">
      {children}
    </Text>
  );
}

/**
 * Every value of a relationship, read-only, under the labels its edit form gives them, and the
 * values the server derives beside them. An empty value is shown as such, never left out.
 */
export function RelationshipRead({
  rel: r,
  tableDomainById,
  domainsEnabled,
}: {
  rel: Relationship;
  tableDomainById: Record<string, string>;
  domainsEnabled: boolean;
}) {
  const { t } = useTranslation();
  const none = t("relationshipRow.none");
  const or = (v: string | number | null | undefined) =>
    v === null || v === undefined || v === "" ? none : v;
  const yesNo = (v: boolean) => (v ? t("relationshipRow.yes") : t("relationshipRow.no"));
  const isFunction = Boolean(r.targetFunctionName);
  // A junction-backed edge is matched in Cypher by its nomination, not by its alias: the value
  // shown is the type a pattern matches, as the list shows it.
  const cql = r.viaTableName
    ? or(cypherRelType(r))
    : (r.alias ?? (
        <Text span c="dimmed" fs="italic">
          {or(r.computedCypherAlias)}
        </Text>
      ));
  const labelSource: Record<string, string> = {
    column: t("addRelationshipForm.viaLabelSourceColumn"),
    table: t("addRelationshipForm.viaLabelSourceTable"),
    fixed: t("addRelationshipForm.viaLabelSourceFixed"),
  };
  const domain = (id: string | null | undefined) => or(id ? domainToSqlName(id) : null);
  return (
    <div className="detail-groups" data-testid="relationship-read">
      <DetailFields
        testId="relationship-read-names"
        fields={[
          { label: t("relationshipRow.aliasLabel"), value: <code>{cql}</code> },
          {
            label: t("relationshipRow.graphqlAliasLabel"),
            value: <code>{or(r.graphqlAlias)}</code>,
          },
          { label: t("relationshipRow.sqlName"), value: <code>{or(r.physicalName)}</code> },
          { label: t("relationshipRow.autoTracked"), value: yesNo(r.autoSuggested) },
        ]}
      />
      <ReadHeading>{t("relationshipRow.source")}</ReadHeading>
      <DetailFields
        testId="relationship-read-source"
        fields={[
          ...(domainsEnabled
            ? [{ label: t("relationshipRow.domainLabel"), value: domain(r.sourceDomainId) }]
            : []),
          { label: t("relationshipRow.tableLabel"), value: or(r.sourceTableName) },
          { label: t("relationshipRow.columnLabel"), value: or(r.sourceColumn) },
        ]}
      />
      <ReadHeading>{t("relationshipRow.target")}</ReadHeading>
      <DetailFields
        testId="relationship-read-target"
        fields={[
          {
            label: t("relationshipRow.targetTypeLabel"),
            value: isFunction
              ? t("relationshipRow.targetTypeFunction")
              : t("relationshipRow.targetTypeTable"),
          },
          ...(isFunction
            ? [
                { label: t("relationshipRow.functionLabel"), value: or(r.targetFunctionName) },
                { label: t("relationshipRow.functionArgLabel"), value: or(r.functionArg) },
              ]
            : [
                ...(domainsEnabled
                  ? [
                      {
                        label: t("relationshipRow.domainLabel"),
                        value: or(
                          r.targetTableId !== null ? tableDomainById[r.targetTableId] : null,
                        ),
                      },
                    ]
                  : []),
                { label: t("relationshipRow.tableLabel"), value: or(r.targetTableName) },
                { label: t("relationshipRow.columnLabel"), value: or(r.targetColumn) },
                { label: t("relationshipRow.cardinality"), value: or(r.cardinality) },
              ]),
          { label: t("relationshipRow.materializeCheckbox"), value: yesNo(r.materialize) },
          { label: t("relationshipRow.excludeFromCypher"), value: yesNo(r.disableCypher) },
          { label: t("relationshipRow.sameEntity"), value: yesNo(r.sameEntity) },
          {
            label: t("relationshipRow.refreshIntervalSeconds"),
            value: r.materialize ? or(r.refreshInterval) : none,
          },
        ]}
      />
      {r.parameters.length > 0 && (
        <>
          <ReadHeading>{t("relationshipRow.parametersFed")}</ReadHeading>
          <DetailFields
            testId="relationship-read-parameters"
            fields={r.parameters.map((p) => ({ label: p.parameter, value: p.sourceColumn }))}
          />
        </>
      )}
      {!isFunction && (
        <>
          <ReadHeading>{t("addRelationshipForm.junctionTitle")}</ReadHeading>
          <DetailFields
            testId="relationship-read-junction"
            fields={[
              { label: t("addRelationshipForm.viaTableLabel"), value: or(r.viaTableName) },
              {
                label: t("addRelationshipForm.viaSourceColumnLabel"),
                value: or(r.viaSourceColumn),
              },
              {
                label: t("addRelationshipForm.viaTargetColumnLabel"),
                value: or(r.viaTargetColumn),
              },
              { label: t("addRelationshipForm.viaTypeColumnLabel"), value: or(r.viaTypeColumn) },
              { label: t("addRelationshipForm.viaTypeValueLabel"), value: or(r.viaTypeValue) },
              {
                label: t("addRelationshipForm.viaLabelSourceLabel"),
                value: r.viaLabelSource ? labelSource[r.viaLabelSource] : none,
              },
            ]}
          />
        </>
      )}
    </div>
  );
}
