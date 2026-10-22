// Copyright (c) 2026 Kenneth Stott
// Canary: 29e0d0e5-198d-4626-8c25-33c2ca93a88c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ReactNode } from "react";
import type { TFunction } from "i18next";
import type { RegisteredTable } from "../../types/admin";
import type { DetailField } from "../../components/detail/DetailFields";
import { PAGING_READS } from "./paging";
import { unset, yesNo } from "./tableSectionFields";

// What the Load and timeliness tab reads of a table's stored settings, a list of fields per part:
// each under the label its edit control has, "—" when nothing is stored.

/** A stored value as it reads; nothing stored is DetailFields' one empty mark. */
function orDash(value: ReactNode | null | undefined): ReactNode {
  return value ?? null;
}

function code(value: string | null | undefined): ReactNode {
  return value ? <code>{value}</code> : null;
}

/** REQ-318: the table's paging as declared. Only a table the source pages has any. */
export function pagingFields(t: TFunction, table: RegisteredTable): DetailField[] {
  if (table.pagingKind === null) return [];
  const p = table.pagination;
  if (table.pagingKind === "connection")
    return [{ label: t("tableEditForm.pagingMaxRows"), value: orDash(p?.maxRows) }];
  const type = p?.type ?? null;
  return [
    {
      label: t("tableEditForm.pagingType"),
      value: type ? t(`tableEditForm.pagingTypes.${type}`) : unset(t("tableEditForm.pagingNone")),
    },
    ...(type
      ? PAGING_READS[type].map(({ field, label = field }) => ({
          label: t(`tableEditForm.pagingFields.${label}`),
          value: orDash(p?.[field] as string | number | null),
        }))
      : []),
    ...(type
      ? [{ label: t("tableEditForm.pagingFields.maxPages"), value: orDash(p?.maxPages) }]
      : []),
    { label: t("tableEditForm.pagingFields.rowsField"), value: code(p?.rowsField) },
  ];
}

/** Live delivery, when the table has it. */
export function liveFields(t: TFunction, table: RegisteredTable): DetailField[] {
  const live = table.live;
  if (!live) return [];
  const sink = live.outputs.find((o) => o.type === "kafka") ?? null;
  const consumes = live.strategy === "kafka" || live.kafka != null;
  return [
    {
      label: t("liveDeliveryFieldset.queryIdLabel"),
      value: <code>{`${table.sourceId}.${table.tableName}`}</code>,
    },
    { label: t("liveDeliveryFieldset.watermarkColumnLabel"), value: code(live.watermarkColumn) },
    { label: t("liveDeliveryFieldset.pollIntervalLabel"), value: String(live.pollInterval) },
    ...(consumes
      ? [
          {
            label: t("liveDeliveryFieldset.kafkaConsumeTopicLabel"),
            value: code(live.kafka?.topic),
          },
          {
            label: t("liveDeliveryFieldset.kafkaConsumeFormatLabel"),
            value: orDash(live.kafka?.format),
          },
          {
            label: t("liveDeliveryFieldset.kafkaConsumeKeyColumnLabel"),
            value: code(live.kafka?.keyColumn),
          },
          {
            label: t("liveDeliveryFieldset.kafkaLandingTimeLabel"),
            value: code(live.kafka?.landingTimeColumn),
          },
        ]
      : []),
    // Every live table fans out over SSE; Kafka is the one output that is chosen.
    { label: t("liveDeliveryFieldset.sseFanoutLabel"), value: yesNo(t, true) },
    { label: t("liveDeliveryFieldset.kafkaOutputLabel"), value: yesNo(t, sink != null) },
    ...(sink
      ? [
          {
            label: t("liveDeliveryFieldset.kafkaBootstrapLabel"),
            value: code(sink.bootstrapServers),
          },
          { label: t("liveDeliveryFieldset.kafkaTopicLabel"), value: code(sink.topic) },
          { label: t("liveDeliveryFieldset.kafkaKeyColumnLabel"), value: code(sink.keyColumn) },
        ]
      : []),
  ];
}

/** REQ-874: incremental reload, when the table declares it. */
export function deltaFields(t: TFunction, table: RegisteredTable): DetailField[] {
  const d = table.delta;
  if (!d) return [];
  return [
    {
      label: t("tableEditForm.delta.apply"),
      value:
        d.apply === "append"
          ? t("tableEditForm.delta.applyAppend")
          : t("tableEditForm.delta.applyUpsert"),
    },
    {
      label: t("tableEditForm.delta.deletes"),
      value:
        d.deletes === "tombstone"
          ? t("tableEditForm.delta.deletesTombstone")
          : t("tableEditForm.delta.deletesNone"),
    },
    ...(d.deletes === "tombstone"
      ? [{ label: t("tableEditForm.delta.tombstoneColumn"), value: code(d.tombstoneColumn) }]
      : []),
    { label: t("tableEditForm.delta.rebuildEvery"), value: orDash(d.rebuildEvery) },
    {
      label: t("tableEditForm.delta.query"),
      value: d.query ? <code className="detail-code">{d.query}</code> : null,
    },
  ];
}

/** REQ-2109: the business key, valid time and recorded time a table or view keeps its versions
 * by, when it declares them. */
export function historyFields(t: TFunction, table: RegisteredTable): DetailField[] {
  const on = table.businessKey.length > 0;
  return [
    {
      label: t("tableEditForm.timeTravelLabel"),
      value: t(on ? "tableEditForm.timeTravelBitemporal" : "tableEditForm.timeTravelOff"),
    },
    ...(on
      ? [
          { label: t("tableEditForm.businessKeyLabel"), value: table.businessKey.join(", ") },
          { label: t("tableEditForm.validTimeLabel"), value: orDash(table.validTime ?? "") },
          { label: t("tableEditForm.recordedTimeLabel"), value: orDash(table.recordedTime ?? "") },
        ]
      : []),
  ];
}

/** A materialized view's store: how it keeps its rows, and what runs before a refresh. */
export function mvStoreFields(t: TFunction, table: RegisteredTable): DetailField[] {
  if (!(table.viewSql != null && table.materialize)) return [];
  const timeTravel = table.businessKey.length > 0;
  return [
    ...(timeTravel
      ? []
      : [
          {
            label: t("tableEditForm.persistLabel"),
            value: t(
              table.mvPersist === "upsert"
                ? "tableEditForm.storeUpsert"
                : "tableEditForm.storeReplace",
            ),
          },
        ]),
    ...historyFields(t, table),
    { label: t("tableEditForm.incrementalLabel"), value: yesNo(t, table.mvIncremental) },
    {
      label: t("tableEditForm.preprocessLabel"),
      value: table.mvPreprocess ? <code className="detail-code">{table.mvPreprocess}</code> : null,
    },
  ];
}

/** REQ-962/1168: a materialized view's snapshot schedule. */
export function snapshotFields(t: TFunction, table: RegisteredTable): DetailField[] {
  if (!(table.viewSql != null && table.materialize)) return [];
  const verifyAll = table.mvExpectedEvents == null;
  return [
    { label: t("tableEditForm.calendarLabel"), value: orDash(table.mvCalendar) },
    {
      label: t("tableEditForm.grainLabel"),
      value: table.mvGrain ? table.mvGrain.replace(/^RRULE:/i, "RECURS:") : null,
    },
    { label: t("tableEditForm.allowedLatenessLabel"), value: String(table.mvAllowedLateness) },
    { label: t("tableEditForm.businessDayGrainLabel"), value: yesNo(t, table.mvBusinessDayGrain) },
    { label: t("tableEditForm.expectedAllLabel"), value: yesNo(t, verifyAll) },
    ...(verifyAll
      ? []
      : [
          {
            label: t("tableEditForm.expectedEventsLabel"),
            value: orDash((table.mvExpectedEvents ?? []).join(", ")),
          },
        ]),
  ];
}

/** REQ-1141: the window a load-protected table is read in, its two ends. */
export function offPeakFields(t: TFunction, table: RegisteredTable): DetailField[] {
  const [opens = "", closes = ""] = (table.offPeakWindow ?? "").split("-");
  const window = t("tableEditForm.offPeakWindowLabel");
  return [
    { label: `${window} · ${t("tableEditForm.offPeakOpens")}`, value: orDash(opens) },
    { label: `${window} · ${t("tableEditForm.offPeakCloses")}`, value: orDash(closes) },
    { label: t("tableEditForm.offPeakTzLabel"), value: orDash(table.offPeakTz) },
  ];
}
