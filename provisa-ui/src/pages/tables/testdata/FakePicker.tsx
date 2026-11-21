// Copyright (c) 2026 Kenneth Stott
// Canary: daab3203-35a2-4a4b-af64-00a9415b9480
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Select, TextInput } from "@mantine/core";
import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import type { FakeCatalog } from "../../../api/fakes";
import { chosen, compose, offeredFor, parseArgs, type ArgValues, type Choice } from "./declaration";
import { FieldGroup, FormField, FormLayout } from "../../../components/form/FormLayout";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";

// REQ-1494: a kind of fake or fake method picked by name within its category -- among those whose
// values a column of `family` can hold, as the server's checks answer -- its arguments filled in,
// written as the text the column saves. That text, "Saved as", stays editable by hand; it is
// folded under "Edit as text" until a choice is made or the reader opens it.
export function FakePicker({
  catalog,
  family,
  label,
  value,
  onChange,
  rule,
  testId,
  children,
}: {
  catalog: FakeCatalog;
  // The column's family, as the server names it (FakeCatalog.typeFamilies).
  family: string;
  label: string;
  value: string;
  onChange: (declaration: string) => void;
  // A synthetic rule may also be a cross-row kind; a fake may not.
  rule: boolean;
  testId: string;
  // What belongs to this section beneath its text (the fake's Stable).
  children?: ReactNode;
}) {
  const { t } = useTranslation();
  const [choice, setChoice] = useState<Choice | null>(chosen(catalog, value));
  const [args, setArgs] = useState<ArgValues>(() => parseArgs(catalog, value));
  // Once there is text it stays in view, even when it is cleared to be written again.
  const [written, setWritten] = useState(value.trim() !== "");
  const groups = new Map<string, { value: string; label: string }[]>();
  for (const k of catalog.kinds) {
    if (!offeredFor(k, family, rule)) continue;
    const group = t(`testData.kindCategory.${k.category}`);
    groups.set(group, [...(groups.get(group) ?? []), { value: `kind:${k.name}`, label: k.name }]);
  }
  for (const m of catalog.methods) {
    if (!offeredFor(m, family, rule)) continue;
    const group = t("testData.methodCategory", { category: m.category });
    groups.set(group, [...(groups.get(group) ?? []), { value: `method:${m.name}`, label: m.name }]);
  }
  const data = [...groups].map(([group, items]) => ({ group, items }));
  const argNames: string[] =
    choice === null
      ? []
      : choice.type === "kind"
        ? (catalog.kinds.find((k) => k.name === choice.name)?.args ?? []).map((a) => a.name)
        : (catalog.methods.find((m) => m.name === choice.name)?.params ?? []).map((p) => p.name);
  const pick = (next: Choice | null, nextArgs: ArgValues) => {
    setChoice(next);
    setArgs(nextArgs);
    if (next) setWritten(true);
    onChange(next ? compose(catalog, next, nextArgs) : "");
  };
  return (
    // One section: its list, the chosen entry's arguments, then the text it saves.
    <FormLayout data-testid={`${testId}-section`}>
      <FormField size="medium">
        <Select
          label={label}
          searchable
          clearable
          data={data}
          value={choice ? `${choice.type}:${choice.name}` : null}
          onChange={(v) => {
            const [type, name] = (v ?? ":").split(":");
            pick(v ? { type: type as "kind" | "method", name } : null, {});
          }}
          comboboxProps={{ withinPortal: true }}
          data-testid={`${testId}-pick`}
        />
      </FormField>
      {argNames.length > 0 && (
        // The chosen kind's or method's arguments, together.
        <FieldGroup>
          {argNames.map((a) => (
            <FormField size="short" key={a}>
              <TextInput
                label={a}
                value={args[a] ?? ""}
                onChange={(e) => choice && pick(choice, { ...args, [a]: e.target.value })}
                data-testid={`${testId}-arg-${a}`}
              />
            </FormField>
          ))}
        </FieldGroup>
      )}
      <FormField size="full">
        <Collapsibles id={`testdata.${testId}`}>
          <CollapsibleItem
            value="saved-as"
            title={t("testData.editAsText")}
            defaultOpen={false}
            forceOpen={written || value.trim() !== ""}
            mountWhenOpen
            controlTestId={`${testId}-edit-as-text`}
          >
            <FormLayout>
              <FormField size="long" hint={t("testData.savedAsHint")}>
                <TextInput
                  label={t("testData.savedAs")}
                  value={value}
                  onChange={(e) => {
                    setChoice(chosen(catalog, e.target.value));
                    setArgs(parseArgs(catalog, e.target.value));
                    setWritten(true);
                    onChange(e.target.value);
                  }}
                  data-testid={`${testId}-text`}
                />
              </FormField>
            </FormLayout>
          </CollapsibleItem>
        </Collapsibles>
      </FormField>
      {children}
    </FormLayout>
  );
}
