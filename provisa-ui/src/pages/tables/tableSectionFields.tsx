// Copyright (c) 2026 Kenneth Stott
// Canary: b467743d-212f-4bce-af41-82de822ddf70
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment } from "react";
import { Link } from "react-router-dom";
import { Text } from "@mantine/core";
import type { TFunction } from "i18next";
import type { DataProduct, RegisteredTable, Source } from "../../types/admin";
import { DQ_CHECKERS } from "../../types/admin";
import type { DetailField } from "../../components/detail/DetailFields";
import { detailHref } from "../../components/list/useDetailParam";

// What a table's sections say of a field, shared by the read view and the edit form.

/** The change signals a table may choose, as the edit form lists them. */
export function changeSignalChoices(t: TFunction) {
  return [
    { value: "", label: t("tableEditForm.csInherit") },
    { value: "ttl", label: t("tableEditForm.csTtl") },
    { value: "probe", label: t("tableEditForm.csProbe") },
    { value: "ttl_probe", label: t("tableEditForm.csTtlProbe") },
    { value: "native", label: t("tableEditForm.csNative") },
    { value: "debezium", label: t("tableEditForm.csDebezium") },
    { value: "kafka", label: t("tableEditForm.csKafka") },
  ];
}

/** Whether the table's source is a data-quality checker: its table holds that checker's scans. */
export function isCheckerSource(source: Source | undefined): boolean {
  return (
    source != null && (DQ_CHECKERS as readonly string[]).includes((source.type ?? "").toLowerCase())
  );
}

/** Whether a source of this type serves files, where one table may span a glob of them (REQ-788). */
export function isFilesSourceType(sourceType: string): boolean {
  return ["files", "csv", "parquet"].includes(sourceType);
}

/** The source types whose table is a query's projection, not a relation of the source. */
const QUERY_API_TYPES = ["neo4j", "sparql"];

/**
 * Whether a table's columns can be read from its live source (REQ-252): not a view's (its SQL
 * gives them), a data-quality checker's (its contract does), a Data Profiler result's (its scan
 * does), nor a table a query produces (a Cypher or SPARQL projection, a native pipeline or search):
 * the same tables Register Table offers it for.
 */
export function discoversColumns(
  sourceType: string,
  table: Pick<RegisteredTable, "viewSql" | "viewMetrics" | "queryTemplate">,
): boolean {
  if (table.viewSql != null || table.viewMetrics != null || table.queryTemplate) return false;
  if ((DQ_CHECKERS as readonly string[]).includes(sourceType)) return false;
  return sourceType !== "data_profiler" && !QUERY_API_TYPES.includes(sourceType);
}

/** A value that is not set, said in the dimmed text of a default. */
export function unset(text: string) {
  return (
    <Text span size="sm" c="dimmed">
      {text}
    </Text>
  );
}

/** A tick read as words. */
export function yesNo(t: TFunction, value: boolean) {
  return value ? t("detailFields.yes") : t("detailFields.no");
}

/** The data products line of Associations, read-only: each product the table belongs to, a link
 * that opens it, or that it belongs to none. Membership is set on the product (REQ-1634). */
export function dataProductField(
  t: TFunction,
  table: RegisteredTable,
  dataProducts: DataProduct[],
): DetailField {
  return {
    label: t("tableEditForm.dataProductLabel"),
    testId: "table-read-view-product",
    value:
      table.productIds.length === 0 ? (
        unset(t("tableEditForm.dataProductNone"))
      ) : (
        <>
          {table.productIds.map((id, i) => (
            <Fragment key={id}>
              {i > 0 && ", "}
              <Link to={detailHref("products", id)}>
                {dataProducts.find((p) => p.id === id)?.name ?? id}
              </Link>
            </Fragment>
          ))}
        </>
      ),
  };
}
