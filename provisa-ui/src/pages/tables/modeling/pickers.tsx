// Copyright (c) 2026 Kenneth Stott
// Canary: 5d5f9be2-a161-4df6-832d-2bd284aa89f0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: the pickers the modeling wizards ask with -- the entity/fact form's: a registered
// table of the user's own as a source, its columns (those that can be a key, those that can be a
// measure), and the domain the model is put into. Each is one field of the shared form layout.

import { useTranslation } from "react-i18next";
import { MultiSelect, Select, TextInput } from "@mantine/core";
import type { Domain } from "../../../types/admin";
import { FormField, type FormFieldSize } from "../../../components/form/FormLayout";
import { assignableDomainIds } from "../../../lib/internalDomains";

interface Common {
  label: string;
  help?: string;
  hint?: string;
  required?: boolean;
  size?: FormFieldSize;
  testId: string;
}

export function TextField({
  label,
  help,
  hint,
  required,
  size = "medium",
  testId,
  value,
  onChange,
  placeholder,
}: Common & { value: string; onChange: (v: string) => void; placeholder?: string }) {
  return (
    <FormField size={size} help={help} hint={hint}>
      <TextInput
        label={label}
        required={required}
        value={value}
        onChange={(e) => onChange(e.currentTarget.value)}
        placeholder={placeholder}
        data-testid={testId}
      />
    </FormField>
  );
}

export function ChoiceField({
  label,
  help,
  hint,
  required,
  size = "medium",
  testId,
  value,
  onChange,
  data,
  placeholder,
  disabled,
}: Common & {
  value: string | null;
  onChange: (v: string | null) => void;
  data: { value: string; label: string }[];
  placeholder?: string;
  disabled?: boolean;
}) {
  return (
    <FormField size={size} help={help} hint={hint}>
      <Select
        label={label}
        required={required}
        searchable
        data={data}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        disabled={disabled}
        comboboxProps={{ withinPortal: true }}
        data-testid={testId}
      />
    </FormField>
  );
}

export function ChoicesField({
  label,
  help,
  hint,
  required,
  size = "long",
  testId,
  value,
  onChange,
  data,
  placeholder,
  disabled,
}: Common & {
  value: string[];
  onChange: (v: string[]) => void;
  data: { value: string; label: string; disabled?: boolean }[];
  placeholder?: string;
  disabled?: boolean;
}) {
  return (
    <FormField size={size} help={help} hint={hint}>
      <MultiSelect
        label={label}
        required={required}
        searchable
        data={data}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        disabled={disabled}
        comboboxProps={{ withinPortal: true }}
        data-testid={testId}
      />
    </FormField>
  );
}

export function DomainField({
  domains,
  value,
  onChange,
  testId,
}: {
  domains: Domain[];
  value: string;
  onChange: (v: string) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <ChoiceField
      label={t("modelingForm.domainLabel")}
      help={t("modelingWizard.domainHelp")}
      required
      testId={testId}
      data={assignableDomainIds(domains.map((d) => d.id)).map((id) => ({ value: id, label: id }))}
      value={value || null}
      onChange={(v) => onChange(v ?? "")}
    />
  );
}
