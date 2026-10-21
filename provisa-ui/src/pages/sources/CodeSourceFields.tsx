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
// comes from: a package at an exact version, a Git repository at a commit, or an uploaded archive,
// with a lockfile beside it when the code carries none. The server installs and
// reads the code when the source is saved, and refuses code it cannot use by name.
// REQ-2104: the org secrets its code may read through Provisa's services, picked by name.
// REQ-2105: the monthly cap on what its calls to Provisa's LLM service cost.
// REQ-2106: its non-secret settings, which its code reads through Provisa's services.

import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  Button,
  FileInput,
  MultiSelect,
  NumberInput,
  Select,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import { uploadCodeArchive, uploadLockfile } from "../../api/codeUploads";
import { fetchSecrets } from "../../api/secrets";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import { useFormRead } from "../../components/form/formRead";
import { CodeExports } from "./CodeExports";
import { VscodeLink } from "./VscodeLink";
import {
  NEW_SOURCE,
  canKeepFolders,
  filesOfHandle,
  filesOfInput,
  heldFolder,
  pickFolder,
  zipFolder,
  type FolderFile,
} from "./folderZip";
import { CodeHostPanel } from "./CodeHostPanel";
import { useAuth } from "../../context/AuthContext";
import {
  LOCKFILES,
  NODE_RUNTIMES,
  PYTHON,
  declaredSecrets,
  vscodeLink,
  withSecretNames,
  type CodeOrigin,
} from "./codeSource";

interface Props {
  type: string;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
  /** REQ-2098: the source being edited; a new one has no image or remote host yet. */
  sourceId?: string;
}

/** REQ-2098: a built source's code fields, shown but not edited (its code is changed in dev).
 * Read, the form is not edited anyway, and its rows stay directly in the grid it draws. */
function CodeLocked({ locked, children }: { locked: boolean; children: ReactNode }) {
  const read = useFormRead();
  if (!locked || read) return <>{children}</>;
  return (
    <fieldset disabled style={{ border: 0, padding: 0, margin: 0, minWidth: 0 }}>
      {children}
    </fieldset>
  );
}

export function CodeSourceFields({ type, fields, setFields, sourceId }: Props) {
  const { t } = useTranslation();
  const { activeOrgId, multitenancy } = useAuth();
  const read = useFormRead();
  // REQ-2121: the folder chosen for this source's code, kept by the browser where it can be.
  const folderKey = sourceId ?? NEW_SOURCE;
  const folderInput = useRef<HTMLInputElement>(null);
  const [folderChosen, setFolderChosen] = useState(false);
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

  // What is kept: its file's name with the start of its hash beside it, or the hash alone.
  // REQ-2121: Choose folder zips the folder here and uploads it as the source's archive.
  const sendFolder = async (name: string, files: FolderFile[]) => {
    if (!activeOrgId) return;
    const file = zipFolder(name, files);
    const hash = await uploadCodeArchive(activeOrgId, file);
    set({ code_upload: hash, code_upload_filename: file.name });
    setFolderChosen(true);
  };
  const chooseFolder = async () => {
    if (!canKeepFolders()) {
      folderInput.current?.click(); // folderPicked takes it from there
      return;
    }
    setUploading("code");
    setFailed(null);
    try {
      const handle = await pickFolder(folderKey);
      await sendFolder(handle.name, await filesOfHandle(handle));
    } catch (e) {
      // Closing the folder picker chooses nothing; anything else is said beside the field.
      if (!(e instanceof DOMException && e.name === "AbortError")) {
        setFailed({ key: "code", text: String(e instanceof Error ? e.message : e) });
      }
    } finally {
      setUploading("");
    }
  };
  const folderPicked = async (files: File[]) => {
    if (!files.length) return;
    setUploading("code");
    setFailed(null);
    try {
      const picked = await filesOfInput(files);
      await sendFolder(picked.name, picked.files);
    } catch (e) {
      setFailed({ key: "code", text: String(e instanceof Error ? e.message : e) });
    } finally {
      setUploading("");
    }
  };
  const vscode = vscodeLink(fields);

  const kept = (hash: string, name: string) =>
    !hash
      ? undefined
      : name
        ? t("codeSourceFields.keptWithHash", { name, hash: hash.slice(0, 12) })
        : t("codeSourceFields.kept", { name: hash.slice(0, 12) });

  // REQ-2098: a built source runs the code its image was built from; its code is changed in dev.
  const built = fields.code_mode === "built";
  const saved = Boolean(sourceId && activeOrgId && fields.code_content_hash);

  return (
    <>
      {built && (
        <Text size="sm" c="dimmed" data-testid="code-built-note">
          {t("codeHost.builtCodeNote")}
        </Text>
      )}
      <CodeLocked locked={built}>
        <FieldGroup>
          {!python && (
            <FormField size="short">
              <Select
                label={t("codeSourceFields.runtime")}
                data={NODE_RUNTIMES.map((r) => ({
                  value: r,
                  label: t(`codeSourceFields.runtime_${r}`),
                }))}
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
                {
                  value: "package",
                  label: t(python ? "codeSourceFields.pypi" : "codeSourceFields.npm"),
                },
                { value: "git", label: t("codeSourceFields.git") },
                { value: "upload", label: t("codeSourceFields.upload") },
                // REQ-2121: a directory on the server, on a single-tenant install alone.
                ...(multitenancy
                  ? []
                  : [{ value: "directory", label: t("codeSourceFields.directoryOrigin") }]),
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
                data-kept={kept(fields.code_upload ?? "", fields.code_upload_filename ?? "")}
                accept=".zip,.tgz,.tar.gz"
                disabled={uploading === "code"}
                onChange={(file) => void upload("code", file)}
                error={failed?.key === "code" ? failed.text : undefined}
                data-testid="code-archive"
              />
            </FormField>
            {!read && (
              <Button
                variant="default"
                disabled={uploading === "code"}
                onClick={() => void chooseFolder()}
                data-testid="code-choose-folder"
              >
                {t("codeSourceFields.chooseFolder")}
              </Button>
            )}
            {!read && (
              <input
                ref={folderInput}
                type="file"
                hidden
                {...{ webkitdirectory: "", directory: "" }}
                onChange={(e) => void folderPicked([...(e.currentTarget.files ?? [])])}
                data-testid="code-folder-input"
              />
            )}
            {!read && (heldFolder(folderKey) || folderChosen) && (
              <Text size="xs" c="dimmed" data-testid="code-folder-note">
                {t(
                  heldFolder(folderKey)
                    ? "codeSourceFields.folderKept"
                    : "codeSourceFields.folderAgain",
                )}
              </Text>
            )}
          </FieldGroup>
        )}

        {origin === "directory" && (
          <FieldGroup>
            <FormField size="long" hint={t("codeSourceFields.directoryHint")}>
              <TextInput
                required
                label={t("codeSourceFields.directory")}
                placeholder="/srv/code/rates"
                value={fields.code_directory ?? ""}
                onChange={(e) => set({ code_directory: e.currentTarget.value })}
                data-testid="code-directory"
              />
            </FormField>
          </FieldGroup>
        )}

        {vscode && (
          <FieldGroup>
            <VscodeLink href={vscode} />
          </FieldGroup>
        )}

        {sourceId && activeOrgId && (
          <FieldGroup row>
            <CodeExports orgId={activeOrgId} sourceId={sourceId} />
          </FieldGroup>
        )}

        <FieldGroup>
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
              data-kept={kept(fields.lockfile_upload ?? "", fields.lockfile_filename ?? "")}
              accept={LOCKFILES[type].join(",")}
              disabled={uploading === "lockfile"}
              onChange={(file) =>
                file
                  ? void upload("lockfile", file)
                  : set({ lockfile_upload: "", lockfile_filename: "" })
              }
              error={failed?.key === "lockfile" ? failed.text : undefined}
              data-testid="code-lockfile"
            />
          </FormField>
        </FieldGroup>

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
          <FormField size="short" help={t("codeSourceFields.llmCapHint")}>
            <NumberInput
              label={t("codeSourceFields.llmCap")}
              placeholder={t("codeSourceFields.llmCapPlaceholder")}
              min={0}
              decimalScale={2}
              prefix="$"
              value={fields.code_llm_cap ?? ""}
              onChange={(v) => set({ code_llm_cap: v === "" ? "" : String(v) })}
              data-testid="code-llm-cap"
            />
          </FormField>
        </FieldGroup>
        <FieldGroup>
          <FormField size="long" help={t("codeSourceFields.settingsHint")}>
            <Textarea
              label={t("codeSourceFields.settings")}
              placeholder={"region = eu-west\nthreshold = 0.8"}
              value={fields.code_settings ?? ""}
              onChange={(e) => set({ code_settings: e.currentTarget.value })}
              autosize
              minRows={2}
              maxRows={10}
              styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
              data-testid="code-settings"
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
      </CodeLocked>
      {saved && (
        <CodeHostPanel
          orgId={activeOrgId as string}
          sourceId={sourceId as string}
          fields={fields}
          setFields={setFields}
        />
      )}
    </>
  );
}
