// Copyright (c) 2026 Kenneth Stott
// Canary: 1072b7a8-61d1-4d5b-a06a-4c2337eced9e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-788: files combined into one table, before it is registered: the files matched, the columns
// they share, the files whose columns differ (named), the files a derived glob adds to those
// picked (accepted, or the glob typed instead), the new table's own name and an optional column
// carrying each row's file.

import { Checkbox, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormChecks, FormField } from "../../components/form/FormLayout";
import type { FileGlobPreview, FilesDiffer } from "./useFileGlobPreview";

interface CombineFilesSectionProps {
  preview: FileGlobPreview | null;
  checking: boolean;
  error: string | null;
  name: string;
  setName: (v: string) => void;
  sourceFileColumn: string;
  setSourceFileColumn: (v: string) => void;
  includeExtra: boolean;
  setIncludeExtra: (v: boolean) => void;
}

export function CombineFilesSection({
  preview,
  checking,
  error,
  name,
  setName,
  sourceFileColumn,
  setSourceFileColumn,
  includeExtra,
  setIncludeExtra,
}: CombineFilesSectionProps) {
  const { t } = useTranslation();
  return (
    <>
      <FormField size="long" data-testid="register-combined-files">
        <CombinedFiles preview={preview} checking={checking} error={error} />
      </FormField>
      {preview && preview.differing.length > 0 && (
        <FormField size="long" data-testid="register-combined-differing">
          <Text size="sm" c="red">
            {t("combineFiles.differHeading")}
          </Text>
          {preview.differing.map((d) => (
            <Text key={d.file} size="sm" c="red">
              {differenceOf(t, d)}
            </Text>
          ))}
        </FormField>
      )}
      {preview && preview.extraFiles.length > 0 && (
        <FormField size="long" data-testid="register-combined-extra">
          <Text size="sm" c="orange">
            {t("combineFiles.extra", {
              glob: preview.glob,
              files: preview.extraFiles.join(", "),
            })}
          </Text>
        </FormField>
      )}
      <FormField size="medium">
        <TextInput
          required
          label={t("combineFiles.nameLabel")}
          placeholder={t("combineFiles.namePlaceholder")}
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
          data-testid="register-combined-name"
        />
      </FormField>
      <FormField size="medium" hint={t("combineFiles.sourceFileColumnHint")}>
        <TextInput
          label={t("combineFiles.sourceFileColumnLabel")}
          placeholder={t("combineFiles.sourceFileColumnPlaceholder")}
          value={sourceFileColumn}
          onChange={(e) => setSourceFileColumn(e.currentTarget.value)}
          data-testid="register-combined-source-file-column"
        />
      </FormField>
      {preview && preview.extraFiles.length > 0 && (
        <FormChecks>
          <Checkbox
            label={t("combineFiles.includeExtra")}
            checked={includeExtra}
            onChange={(e) => setIncludeExtra(e.currentTarget.checked)}
            data-testid="register-combined-include-extra"
          />
        </FormChecks>
      )}
    </>
  );
}

/** The files matched and the columns they share, or why there is nothing to show yet. */
function CombinedFiles({
  preview,
  checking,
  error,
}: {
  preview: FileGlobPreview | null;
  checking: boolean;
  error: string | null;
}) {
  const { t } = useTranslation();
  if (error) {
    return (
      <Text size="sm" c="red">
        {error}
      </Text>
    );
  }
  if (preview === null || checking) {
    return (
      <Text size="sm" c="dimmed">
        {t("combineFiles.checking")}
      </Text>
    );
  }
  if (preview.unmatchedTables.length > 0) {
    return (
      <Text size="sm" c="red">
        {t("combineFiles.unmatched", { tables: preview.unmatchedTables.join(", ") })}
      </Text>
    );
  }
  if (preview.files.length === 0) {
    return (
      <Text size="sm" c="red">
        {t("combineFiles.noMatch")}
      </Text>
    );
  }
  return (
    <>
      <Text size="sm" fw={500}>
        {t("combineFiles.matched", { count: preview.files.length, glob: preview.glob })}
      </Text>
      <Text size="sm">{preview.files.map((f) => f.file).join(", ")}</Text>
      <Text size="sm">
        {preview.sharedColumns.length > 0
          ? t("combineFiles.sharedColumns", { columns: preview.sharedColumns.join(", ") })
          : t("combineFiles.noSharedColumns")}
      </Text>
    </>
  );
}

/** One file whose columns differ: what it lacks, what it adds, or why it could not be read. */
function differenceOf(t: ReturnType<typeof useTranslation>["t"], d: FilesDiffer): string {
  if (d.error) return t("combineFiles.differUnreadable", { file: d.file, error: d.error });
  const parts = [
    ...(d.missing.length > 0 ? [t("combineFiles.lacks", { columns: d.missing.join(", ") })] : []),
    ...(d.extra.length > 0 ? [t("combineFiles.adds", { columns: d.extra.join(", ") })] : []),
  ];
  return t("combineFiles.differ", { file: d.file, how: parts.join("; ") });
}
