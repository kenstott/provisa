// Copyright (c) 2026 Kenneth Stott
// Canary: 2f90d4b7-6e1c-4a35-9c78-b4a1e3d5f062
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: adding a document is one step, a file or a URL and its readers. The document exists at
// once, and is read and indexed in the background.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FileInput, MultiSelect, SegmentedControl, TextInput } from "@mantine/core";
import { addLibraryDocument, type LibraryDocument } from "../../api/referenceLibrary";
import { scopeChange } from "../../components/admin/referenceScope";
import { FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import { ACCEPT, useReaderOptions } from "./libraryParts";

export function LibraryAddForm({
  onAdded,
  onCancel,
}: {
  onAdded: (d: LibraryDocument) => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const options = useReaderOptions();
  const [mode, setMode] = useState<"file" | "url">("file");
  const [file, setFile] = useState<File | null>(null);
  const [url, setUrl] = useState("");
  const [readers, setReaders] = useState<string[]>([]);
  const [problem, setProblem] = useState("");

  const save = async () => {
    setProblem("");
    if (mode === "file" ? file === null : !url.trim()) {
      setProblem(
        mode === "file" ? t("referenceLibrary.fileRequired") : t("referenceLibrary.urlRequired"),
      );
      return;
    }
    if (readers.length === 0) {
      setProblem(t("referenceLibrary.domainsRequired"));
      return;
    }
    try {
      onAdded(
        await addLibraryDocument(
          mode === "file" && file
            ? { file, domains: readers }
            : { url: url.trim(), domains: readers },
        ),
      );
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <FormLayout>
      <FormFooter
        panel
        onSave={save}
        onCancel={onCancel}
        problem={problem || undefined}
        saveProps={{ "data-testid": "reflib-add-save" }}
        cancelProps={{ "data-testid": "reflib-add-cancel" }}
      />
      <FormField size="full">
        <SegmentedControl
          data-testid="reflib-add-mode"
          value={mode}
          onChange={(v) => setMode(v as "file" | "url")}
          data={[
            { value: "file", label: t("referenceLibrary.byFile") },
            { value: "url", label: t("referenceLibrary.byUrl") },
          ]}
        />
      </FormField>
      {mode === "file" ? (
        <FormField help={t("referenceLibrary.fileHelp")} size="long">
          <FileInput
            label={t("referenceLibrary.file")}
            data-testid="reflib-add-file"
            accept={ACCEPT}
            value={file}
            onChange={setFile}
            clearable
          />
        </FormField>
      ) : (
        <FormField help={t("referenceLibrary.urlBackgroundHelp")} size="long">
          <TextInput
            label={t("referenceLibrary.url")}
            data-testid="reflib-add-url"
            placeholder="https://"
            value={url}
            onChange={(e) => setUrl(e.currentTarget.value)}
          />
        </FormField>
      )}
      <FormField help={t("referenceLibrary.readersHelp")} size="long">
        <MultiSelect
          label={t("referenceLibrary.fieldReaders")}
          data-testid="reflib-add-domains"
          data={options}
          value={readers}
          onChange={(v) => setReaders(scopeChange(readers, v))}
          searchable
        />
      </FormField>
    </FormLayout>
  );
}
