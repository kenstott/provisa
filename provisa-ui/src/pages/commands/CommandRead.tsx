// Copyright (c) 2026 Kenneth Stott
// Canary: f5b0ab20-6bc3-4d64-a2ab-416967afdcdd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A command's read view: every value of the function or webhook, under the labels the form gives
// them, an empty one as the shared empty mark. The form's fields that depend on how a function is
// implemented are shown for that implementation only, as the form shows them.

import { maskSpecLabel, maskToLabel } from "../../lib/masks";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import type {
  ActionArg,
  DatasetColumn,
  InlineField,
  StatementContract,
  TrackedFunction,
  TrackedWebhook,
} from "../../api/actions";
import { DetailFields, type DetailField } from "../../components/detail/DetailFields";
import { IMPL_KINDS } from "./types";

function ReadHeading({ children }: { children: string }) {
  return (
    <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase" mb="xs">
      {children}
    </Text>
  );
}

const roles = (list: readonly string[] | null | undefined) =>
  list && list.length > 0 ? list.join(", ") : null;

const args = (list: readonly ActionArg[]) =>
  list.length > 0 ? list.map((a) => `${a.name}: ${a.type}`).join(", ") : null;

/** Who sees each column of the response and how it is masked, one group per column. */
function ResponseColumns({
  title,
  columns,
  testId,
}: {
  title: string;
  columns: readonly (DatasetColumn | InlineField)[];
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <>
      <ReadHeading>{title}</ReadHeading>
      {columns.length === 0 ? (
        <DetailFields testId={testId} fields={[{ label: title, value: null }]} />
      ) : (
        columns.map((c) => {
          return (
            <Fragment key={c.name}>
              <Text size="sm" fw={600} ff="monospace" mb={4}>
                {`${c.name}: ${c.type}`}
              </Text>
              <DetailFields
                testId={`${testId}-${c.name}`}
                fields={[
                  {
                    // Blank is everyone, as the form's label says.
                    label: t("commandFormFields.colVisibleTo"),
                    value: roles(c.visible_to) ?? t("commandsPage.all"),
                  },
                  {
                    label: t("commandFormFields.colMask"),
                    value:
                      (c.masks ?? []).length === 0
                        ? t("maskList.none")
                        : (c.masks ?? [])
                            .map((e, n) =>
                              t("maskList.entryLine", {
                                n: n + 1,
                                to: maskToLabel(e.to, t),
                                mask: maskSpecLabel(e.mask, t),
                              }),
                            )
                            .join(" · "),
                  },
                ]}
              />
            </Fragment>
          );
        })
      )}
    </>
  );
}

/** What every command has, function or webhook: its name, type, kind, domain and description. */
function useCommonFields(c: TrackedFunction | TrackedWebhook, type: "function" | "webhook") {
  const { t } = useTranslation();
  return {
    head: [
      { label: t("commandsPage.nameLabel"), value: c.name },
      {
        label: t("commandsPage.typeLabel"),
        value: type === "function" ? t("commandsPage.typeFunction") : t("commandsPage.typeWebhook"),
      },
      {
        label: t("commandFormFields.kind"),
        value: c.kind === "query" ? t("commandFormFields.query") : t("commandFormFields.mutation"),
      },
      { label: t("commandFormFields.domain"), value: c.domainId },
    ],
    tail: [
      { label: t("commandFormFields.description"), value: c.description },
      { label: t("commandFormFields.arguments"), value: args(c.arguments) },
    ],
  };
}

export function FunctionRead({
  fn,
  productName,
}: {
  fn: TrackedFunction;
  /** The data product it belongs to, by name; null when it belongs to none. */
  productName: string | null;
}) {
  const { t } = useTranslation();
  const common = useCommonFields(fn, "function");
  const impl = fn.implKind ?? "source_procedure";
  const binding = fn.binding ?? {};
  const bound = (key: string) => {
    const v = binding[key];
    if (v === null || v === undefined) return null;
    return Array.isArray(v) ? v.join(" ") : String(v);
  };
  const yesNo = (v: boolean) => (v ? t("commandsPage.yes") : t("commandsPage.no"));
  const bySource = impl === "source_procedure" || impl === "source_operation";
  // REQ-2001: a statement command is its statement; its kind and written table are shown as the
  // statement declares them, and it has no identity switch or return type to show.
  const isStatement = impl === "statement";
  const implementation: DetailField[] = isStatement
    ? [
        { label: t("commandFormFields.bindingSql"), value: bound("sql") },
        ...contractFields(fn.contract, t),
      ]
    : bySource
      ? [
          { label: t("commandFormFields.source"), value: fn.sourceId },
          { label: t("commandFormFields.schema"), value: fn.schemaName },
          { label: t("commandFormFields.functionName"), value: fn.functionName },
        ]
      : impl === "script"
        ? [{ label: t("commandFormFields.bindingArgv"), value: bound("argv") }]
        : impl === "http"
          ? [
              { label: t("commandFormFields.bindingUrl"), value: bound("url") },
              { label: t("commandFormFields.method"), value: bound("method") },
            ]
          : impl === "grpc"
            ? [
                { label: t("commandFormFields.bindingTarget"), value: bound("target") },
                { label: t("commandFormFields.bindingGrpcMethod"), value: bound("method") },
                { label: t("commandFormFields.bindingTls"), value: yesNo(Boolean(binding.tls)) },
              ]
            : [{ label: t("commandFormFields.bindingCallable"), value: bound("callable") }];
  const toTable = Boolean(fn.returns);
  return (
    <div className="detail-groups" data-testid={`function-read-${fn.name}`}>
      <DetailFields
        testId="function-read-fields"
        fields={[
          ...common.head,
          { label: t("commandFormFields.dataProduct"), value: productName },
          {
            label: t("commandFormFields.implementation"),
            value: IMPL_KINDS.find((k) => k.value === impl)?.label ?? impl,
          },
          ...implementation,
          ...(bySource || isStatement
            ? []
            : [
                {
                  label: t("commandFormFields.materialize"),
                  value: yesNo(Boolean(fn.materialize)),
                },
              ]),
          ...(isStatement
            ? []
            : [
                {
                  label: t("commandFormFields.returnType"),
                  value: toTable
                    ? t("commandFormFields.registeredTable")
                    : t("commandFormFields.datasetColumns"),
                },
              ]),
          ...(toTable ? [{ label: t("commandFormFields.returnsTable"), value: fn.returns }] : []),
          {
            label: t("commandFormFields.visibleTo"),
            value: roles(fn.visibleTo) ?? t("commandsPage.all"),
          },
          { label: t("commandFormFields.writesTable"), value: fn.writesTable },
          {
            label: t("commandFormFields.requiresApproval"),
            value: yesNo(Boolean(fn.requiresApproval)),
          },
          ...common.tail,
        ]}
      />
      {!toTable && (
        <ResponseColumns
          title={t("commandFormFields.outputColumnsTitle")}
          columns={isStatement ? (fn.contract?.outputs ?? []) : (fn.outputColumns ?? [])}
          testId="function-read-columns"
        />
      )}
    </div>
  );
}

/** REQ-2001: what a statement command does, as its statement declares it — or why it no longer
 * resolves against the model. */
function contractFields(
  contract: StatementContract | undefined,
  t: (key: string, options?: Record<string, unknown>) => string,
): DetailField[] {
  if (contract === undefined) return [];
  if (contract.broken !== null) {
    return [{ label: t("commandsPage.contractBroken"), value: contract.broken }];
  }
  const parts: string[] = [];
  if (contract.reads.length > 0) {
    parts.push(t("commandsPage.contractReads", { tables: contract.reads.join(", ") }));
  }
  if (contract.writes !== null) {
    parts.push(
      t(contract.upsert ? "commandsPage.contractUpserts" : "commandsPage.contractWrites", {
        table: contract.writes,
      }),
    );
  }
  parts.push(
    contract.writes !== null &&
      contract.outputs.length === 1 &&
      contract.outputs[0].name === "rows_affected"
      ? t("commandsPage.contractReturnsCount")
      : t("commandsPage.contractReturnsRows", {
          columns: contract.outputs.map((o) => o.name).join(", "),
        }),
  );
  return [
    { label: t("commandsPage.contract"), value: parts.join(", ") },
    {
      label: t("commandsPage.contractArguments"),
      value: contract.arguments.map((a) => `${a.name}: ${a.type}`).join(", ") || null,
    },
    { label: t("commandsPage.contractVersion"), value: contract.version },
  ];
}

export function WebhookRead({ wh }: { wh: TrackedWebhook }) {
  const { t } = useTranslation();
  const common = useCommonFields(wh, "webhook");
  return (
    <div className="detail-groups" data-testid={`webhook-read-${wh.name}`}>
      <DetailFields
        testId="webhook-read-fields"
        fields={[
          ...common.head,
          {
            label: t("commandsPage.detailStatus"),
            value:
              wh.approved === false
                ? t("commandsPage.statusPending")
                : t("commandsPage.statusApproved"),
          },
          { label: t("commandFormFields.url"), value: wh.url },
          { label: t("commandFormFields.method"), value: wh.method },
          { label: t("commandFormFields.timeoutMs"), value: wh.timeoutMs },
          { label: t("commandFormFields.returnsTableOptional"), value: wh.returns },
          {
            label: t("commandFormFields.visibleTo"),
            value: roles(wh.visibleTo) ?? t("commandsPage.all"),
          },
          {
            label: t("commandFormFields.requiresApproval"),
            value: wh.requiresApproval ? t("commandsPage.yes") : t("commandsPage.no"),
          },
          ...common.tail,
        ]}
      />
      {!wh.returns && (
        <ResponseColumns
          title={t("commandFormFields.inlineReturnType")}
          columns={wh.inlineReturnType}
          testId="webhook-read-columns"
        />
      )}
    </div>
  );
}
