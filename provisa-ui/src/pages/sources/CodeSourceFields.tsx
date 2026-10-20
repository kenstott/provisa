// Copyright (c) 2026 Kenneth Stott
// Canary: db47ca1c-7d91-4ac0-8002-38e471156b77
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993: what a Node or Python source's code is -- its runtime, and where the code
// comes from: a package at an exact version (with a lockfile beside it when the package declares
// dependencies), a Git repository at a commit, or an uploaded archive. The server installs and
// reads the code when the source is saved, and refuses code it cannot use by name.
// REQ-2104: the org secrets its code may read through Provisa's services, picked by name.

import { useEffect, useState } from "react";
import { FileInput, MultiSelect, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { uploadCodeArchive, uploadLockfile } from "../../api/codeUploads";
import { fetchSecrets } from "../../api/secrets";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import { useAuth } from "../../context/AuthContext";
import {
  LOCKFILES,
  NODE_RUNTIMES,
  PYTHON,
  declaredSecrets,
  withSecretNames,
  type CodeOrigin,
} from "./codeSource";

interface Props {
  type: string;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

export function CodeSourceFields({ type, fields, setFields }: Props) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [uploading, setUploading] = useState("");
  const [failed, setFailed] = useState<{ key: string; text: string } | null>(null);
  const origin = (fields.code_origin ?? "package") as CodeOrigin;
  const python = type === PYTHON;
  const set = (patch: Record<string, string>) => setFields({ ...fields, ...patch });
  const [orgSecrets, setOrgSecrets] = useState<string[]>([]);
  const [secretsFailed, setSecretsFailed] = useState("");

  useEffect(() => {
    if (!activeOrgId) return;
    let live = true;
    fetchSecrets(activeOrgId, "org")
      .then((s) => live && setOrgSecrets(s.secrets.map((x) => x.name)))
      .catch((e) => live && setSecretsFailed(String(e instanceof Error ? e.message : e)));
    return () => {
      live = false;
    };
  }, [activeOrgId]);

  const declared = Object.keys(declaredSecrets(fields));

  const upload = async (key: "code" | "lockfile", file: File | null) => {
    if (!file || !activeOrgId) return;
    setUploading(key);
    setFailed(null);
    try {
      if (key === "code") {
        const hash = await uploadCodeArchive(activeOrgId, file);
        set({ code_upload: hash, code_upload_filename: file.name });
      } else {
        const sha = await uploadLockfile(activeOrgId, file);
        set({ lockfile_upload: sha, lockfile_filename: file.name });
      }
    } catch (e) {
      setFailed({ key, text: String(e instanceof Error ? e.message : e) });
    } finally {
      setUploading("");
    }
  };

  const kept = (hash: string, name: string) =>
    hash ? t("codeSourceFields.kept", { name: name || hash.slice(0, 12) }) : undefined;

  return (
    <>
      <FieldGroup>
        {!python && (
          <FormField size="short">
            <Select
              label={t("codeSourceFields.runtime")}
              data={NODE_RUNTIMES.map((r) => ({ value: r, label: t(`codeSourceFields.runtime_${r}`) }))}
              value={fields.code_runtime ?? "node"}
              allowDeselect={false}
              onChange={(v) => v && set({ code_runtime: v })}
              data-testid="code-runtime"
            />
          </FormField>
        )}
        <FormField size="medium" help={t("codeSourceFields.originHint")}>
          <Select
            label={t("codeSourceFields.origin")}
            data={[
              { value: "package", label: t(python ? "codeSourceFields.pypi" : "codeSourceFields.npm") },
              { value: "git", label: t("codeSourceFields.git") },
              { value: "upload", label: t("codeSourceFields.upload") },
            ]}
            value={origin}
            allowDeselect={false}
            onChange={(v) => v && set({ code_origin: v })}
            data-testid="code-origin"
          />
        </FormField>
      </FieldGroup>

      {origin === "package" && (
        <FieldGroup>
          <FormField size="medium">
            <TextInput
              required
              label={t("codeSourceFields.package")}
              placeholder={python ? "acme-rates" : "@acme/rates"}
              value={fields.code_package ?? ""}
              onChange={(e) => set({ code_package: e.currentTarget.value })}
              data-testid="code-package"
            />
          </FormField>
          <FormField size="short" hint={t("codeSourceFields.versionHint")}>
            <TextInput
              required
              label={t("codeSourceFields.version")}
              placeholder="1.2.0"
              value={fields.code_version ?? ""}
              onChange={(e) => set({ code_version: e.currentTarget.value })}
              data-testid="code-version"
            />
          </FormField>
          <FormField
            size="medium"
            help={t("codeSourceFields.lockfileHint", { names: LOCKFILES[type].join(", ") })}
          >
            <FileInput
              clearable
              label={t("codeSourceFields.lockfile")}
              placeholder={
                kept(fields.lockfile_upload ?? "", fields.lockfile_filename ?? "") ??
                t("codeSourceFields.lockfilePlaceholder")
              }
              accept={LOCKFILES[type].join(",")}
              disabled={uploading === "lockfile"}
              onChange={(file) =>
                file ? void upload("lockfile", file) : set({ lockfile_upload: "", lockfile_filename: "" })
              }
              error={failed?.key === "lockfile" ? failed.text : undefined}
              data-testid="code-lockfile"
            />
          </FormField>
        </FieldGroup>
      )}

      {origin === "git" && (
        <FieldGroup>
          <FormField size="long">
            <TextInput
              required
              label={t("codeSourceFields.repository")}
              placeholder="https://github.com/acme/rates.git"
              value={fields.code_repository ?? ""}
              onChange={(e) => set({ code_repository: e.currentTarget.value })}
              data-testid="code-repository"
            />
          </FormField>
          <FormField size="medium" hint={t("codeSourceFields.commitHint")}>
            <TextInput
              required
              label={t("codeSourceFields.commit")}
              value={fields.code_commit ?? ""}
              onChange={(e) => set({ code_commit: e.currentTarget.value })}
              data-testid="code-commit"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              label={t("codeSourceFields.subdirectory")}
              placeholder="packages/rates"
              value={fields.code_subdirectory ?? ""}
              onChange={(e) => set({ code_subdirectory: e.currentTarget.value })}
              data-testid="code-subdirectory"
            />
          </FormField>
        </FieldGroup>
      )}

      {origin === "upload" && (
        <FieldGroup>
          <FormField size="long" hint={t("codeSourceFields.archiveHint")}>
            <FileInput
              required
              label={t("codeSourceFields.archive")}
              placeholder={
                kept(fields.code_upload ?? "", fields.code_upload_filename ?? "") ??
                t("codeSourceFields.archivePlaceholder")
              }
              accept=".zip,.tgz,.tar.gz"
              disabled={uploading === "code"}
              onChange={(file) => void upload("code", file)}
              error={failed?.key === "code" ? failed.text : undefined}
              data-testid="code-archive"
            />
          </FormField>
        </FieldGroup>
      )}

      <FieldGroup>
        <FormField size="long" help={t("codeSourceFields.secretsHint")}>
          <MultiSelect
            label={t("codeSourceFields.secrets")}
            placeholder={declared.length ? undefined : t("codeSourceFields.secretsPlaceholder")}
            data={[...new Set([...orgSecrets, ...declared])].sort()}
            value={declared}
            searchable
            clearable
            onChange={(names) => set({ code_secrets: withSecretNames(fields, names) })}
            error={secretsFailed || undefined}
            data-testid="code-secrets"
          />
        </FormField>
      </FieldGroup>

      {python && (
        <FieldGroup>
          <FormField size="medium" help={t("codeSourceFields.entryHint")}>
            <TextInput
              label={t("codeSourceFields.entry")}
              placeholder="rates"
              value={fields.code_entry ?? ""}
              onChange={(e) => set({ code_entry: e.currentTarget.value })}
              data-testid="code-entry"
            />
          </FormField>
        </FieldGroup>
      )}
    </>
  );
}
