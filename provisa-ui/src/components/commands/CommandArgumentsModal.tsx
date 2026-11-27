// Copyright (c) 2026 Kenneth Stott
// Canary: 5a0e8c27-9d13-4b6f-a842-7e1f3b9d6c05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): the arguments of a command's call, asked when a surface is chosen
// on its Query it line. A scalar is an input, read as the argument's type; a relation argument is
// given by picking a table the role reads, writing a query, or pasting a few rows (relationArg.ts).
// Open checks on press (REQ-1968): a required argument not given, or one that does not read as its
// type, is marked and refused by name, and nothing opens.

import { Fragment, useState } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import { Button, Group, Radio, Select, TextInput, Textarea } from "@mantine/core";
import type { ActionArg } from "../../api/actions";
import type { SqlNamedTable } from "../../naming";
import { ModalLayout } from "../modal/ModalLayout";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import {
  isRelationArg,
  type ArgValue,
  type CommandCall,
  type CommandSurface,
} from "./commandQueries";
import {
  RELATION_ROW_CAP,
  queryValue,
  relationSources,
  rowsValue,
  tableValue,
  type RelationSource,
} from "./relationArg";

/** What one argument holds while the form is open. */
interface Draft {
  text: string;
  source: RelationSource;
  table: string | null;
  query: string;
  rows: string;
}

const tableKey = (tb: SqlNamedTable) => `${tb.domainId}.${tb.tableName}`;
const tableLabel = (tb: SqlNamedTable) => `${tb.domainId}.${tb.alias || tb.tableName}`;

/** What a draft gives for its argument, to say whether it was given at all. */
function given(arg: ActionArg, d: Draft): string | null {
  if (!isRelationArg(arg)) return d.text;
  return d.source === "table" ? d.table : d.source === "query" ? d.query : d.rows;
}

type Read = { value: ArgValue } | { problem: string };

/** A scalar's text as its argument's type reads it. */
function scalarRead(arg: ActionArg, text: string, t: TFunction): Read {
  const at = { name: arg.name };
  switch (arg.type) {
    case "Int":
      return /^-?\d+$/.test(text.trim())
        ? { value: { kind: "scalar", value: Number(text) } }
        : { problem: t("commandQuery.notNumber", at) };
    case "Float": {
      const n = Number(text);
      return text.trim() !== "" && !Number.isNaN(n)
        ? { value: { kind: "scalar", value: n } }
        : { problem: t("commandQuery.notNumber", at) };
    }
    case "Boolean":
      return text === "true" || text === "false"
        ? { value: { kind: "scalar", value: text === "true" } }
        : { problem: t("commandQuery.notBoolean", at) };
    case "json":
    case "JSON":
      try {
        return { value: { kind: "scalar", value: JSON.parse(text) } };
      } catch {
        return { problem: t("commandQuery.notJson", at) };
      }
    default:
      return { value: { kind: "scalar", value: text } };
  }
}

export function CommandArgumentsModal({
  call,
  surface,
  tables,
  onOpen,
  onClose,
}: {
  call: CommandCall;
  surface: CommandSurface;
  /** The tables the acting role reads: a relation argument picks one of them. */
  tables: readonly SqlNamedTable[];
  onOpen: (values: Record<string, ArgValue>) => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const [drafts, setDrafts] = useState<Record<string, Draft>>(() =>
    Object.fromEntries(
      call.arguments.map((a) => [
        a.name,
        { text: "", source: relationSources(a)[0], table: null, query: "", rows: "" },
      ]),
    ),
  );
  const [problem, setProblem] = useState<string | null>(null);
  // An argument that declares a default may be left out (REQ-2168); every other is required.
  const requiredArgs = call.arguments.filter((a) => !("default" in a));
  const required = useRequiredFields(
    Object.fromEntries(requiredArgs.map((a) => [a.name, given(a, drafts[a.name])])),
  );
  const isRequired = (a: ActionArg) => requiredArgs.includes(a);
  const set = (name: string, part: Partial<Draft>) =>
    setDrafts((prev) => ({ ...prev, [name]: { ...prev[name], ...part } }));
  const controlProps = (a: ActionArg) => (isRequired(a) ? required.field(a.name) : {});

  const read = (a: ActionArg, d: Draft): Read | null => {
    const text = given(a, d);
    if (text === null || text.trim() === "") return null;
    if (!isRelationArg(a)) return scalarRead(a, d.text, t);
    if (d.source === "table") {
      const tb = tables.find((x) => tableKey(x) === d.table);
      if (!tb) return { problem: t("commandQuery.notReadable", { name: a.name }) };
      return { value: tableValue(a, tb) };
    }
    if (d.source === "query") return { value: queryValue(d.query) };
    const rows = rowsValue(d.rows, a);
    if (rows === "overCap")
      return { problem: t("commandQuery.overCap", { name: a.name, cap: RELATION_ROW_CAP }) };
    if (rows === "notRows") return { problem: t("commandQuery.notRows", { name: a.name }) };
    return { value: rows };
  };

  const handleOpen = () => {
    const complete = required.validate();
    const problems: string[] = [];
    if (!complete) {
      const missing = requiredArgs.filter((a) => {
        const text = given(a, drafts[a.name]);
        return text === null || text.trim() === "";
      });
      problems.push(t("commandQuery.missing", { names: missing.map((a) => a.name).join(", ") }));
    }
    const values: Record<string, ArgValue> = {};
    for (const a of call.arguments) {
      const r = read(a, drafts[a.name]);
      if (r === null) continue;
      if ("problem" in r) problems.push(r.problem);
      else values[a.name] = r.value;
    }
    if (problems.length > 0) {
      setProblem(problems.join(" · "));
      return;
    }
    setProblem(null);
    onOpen(values);
  };

  const sourceLabels: Record<RelationSource, string> = {
    table: t("commandQuery.sourceTable"),
    query: t("commandQuery.sourceQuery"),
    rows: t("commandQuery.sourceRows"),
  };

  return (
    <ModalLayout
      opened
      onClose={onClose}
      title={t("commandQuery.title", {
        name: call.name,
        surface: t(`relationshipRow.queryLinks.surfaces.${surface}`),
      })}
      size="lg"
      data-testid="command-args-modal"
    >
      <FormLayout>
        {call.arguments.map((a) => {
          const d = drafts[a.name];
          if (!isRelationArg(a)) {
            return (
              <FormField size="medium" key={a.name} hint={a.type}>
                <TextInput
                  label={a.name}
                  value={d.text}
                  onChange={(e) => set(a.name, { text: e.currentTarget.value })}
                  data-testid={`command-arg-${a.name}`}
                  {...controlProps(a)}
                />
              </FormField>
            );
          }
          const sources = relationSources(a);
          return (
            <Fragment key={a.name}>
              {sources.length > 1 && (
                <FormField size="medium">
                  <Radio.Group
                    label={t("commandQuery.givenAs", { name: a.name })}
                    value={d.source}
                    onChange={(v) => set(a.name, { source: v as RelationSource })}
                    data-testid={`command-arg-${a.name}-source`}
                  >
                    <Group gap="md" mt={4}>
                      {sources.map((src) => (
                        <Radio key={src} value={src} label={sourceLabels[src]} />
                      ))}
                    </Group>
                  </Radio.Group>
                </FormField>
              )}
              {d.source === "table" && (
                <FormField
                  size="medium"
                  hint={t("commandQuery.tableHint", { cap: RELATION_ROW_CAP })}
                >
                  <Select
                    label={a.name}
                    data={tables.map((tb) => ({ value: tableKey(tb), label: tableLabel(tb) }))}
                    value={d.table}
                    onChange={(v) => set(a.name, { table: v })}
                    searchable
                    data-testid={`command-arg-${a.name}-table`}
                    {...controlProps(a)}
                  />
                </FormField>
              )}
              {d.source === "query" && (
                <FormField
                  size="full"
                  hint={t("commandQuery.queryHint", { cap: RELATION_ROW_CAP })}
                >
                  <Textarea
                    label={a.name}
                    autosize
                    minRows={3}
                    ff="monospace"
                    value={d.query}
                    onChange={(e) => set(a.name, { query: e.currentTarget.value })}
                    data-testid={`command-arg-${a.name}-query`}
                    {...controlProps(a)}
                  />
                </FormField>
              )}
              {d.source === "rows" && (
                <FormField size="full" help={t("commandQuery.rowsHint", { cap: RELATION_ROW_CAP })}>
                  <Textarea
                    label={a.name}
                    autosize
                    minRows={3}
                    ff="monospace"
                    value={d.rows}
                    onChange={(e) => set(a.name, { rows: e.currentTarget.value })}
                    data-testid={`command-arg-${a.name}-rows`}
                    {...controlProps(a)}
                  />
                </FormField>
              )}
            </Fragment>
          );
        })}
        {/* Open is not a save: it keeps its word, first, and Cancel follows. */}
        <FormFooter
          onCancel={onClose}
          problem={problem}
          action={
            <Button onClick={handleOpen} data-testid="command-args-open">
              {t("commandQuery.open")}
            </Button>
          }
        />
      </FormLayout>
    </ModalLayout>
  );
}
