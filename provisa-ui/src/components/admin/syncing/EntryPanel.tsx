// Copyright (c) 2026 Kenneth Stott
// Canary: d1b8fcfc-d568-48bf-b63f-a5f372ed6982
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: one Syncing entry in the page's detail panel. Read, it is its fields drawn read-only,
// and for a data catalog its connection test, Publish now (only while its direction includes
// Export) and the result of its last publish. Edited, it is the same fields with Save and Cancel
// in the panel's head. A type with no run of its own yet is offered none. A data catalog's
// comparison with its catalog (its differences and standing choices, EntrySync) opens below.
// While its direction includes Import, a data catalog offers Run import now and shows its last
// Import run: when, how it was started, its outcome, and what it applied and held with why. A
// refused import schedule is shown on its field.
// Duplicate, beside Edit and Delete, opens the create form filled from the entry (SyncingTab);
// that form says when the entry's credentials have to be entered again (`note`).

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Badge, Button, Group, Stack, Table, Text, Title } from "@mantine/core";
import { Check, Pencil, TriangleAlert } from "lucide-react";
import {
  exportsTo,
  importsFrom,
  SyncingRefused,
  type EntryHealth,
  type ImportRun,
  type ImportRunItem,
  type PublishOutcome,
  type SyncEntry,
  type SyncSecretField,
} from "../../../api/syncing";
import { FormField, FormFooter, FormLayout, FormRead } from "../../form/FormLayout";
import { PanelActions } from "../../list/SplitPane";
import { PanelDelete } from "../../list/PanelDelete";
import { PanelDuplicate } from "../../list/PanelDuplicate";
import { Tip } from "../../Tip";
import { CollapsibleItem, Collapsibles } from "../../collapsible/Collapsible";
import { EntrySync } from "./EntrySync";
import { EntryFields, type EntryFieldsProps, type Secrets } from "./EntryFields";

/** The result of a publish: what it published, and each asset the system refused with why. */
export function LastRun({ outcome }: { outcome: PublishOutcome }) {
  const { t } = useTranslation();
  return (
    <Stack gap={4} data-testid="syncing-last-run">
      <Title order={6}>{t("syncingTab.lastRun")}</Title>
      <Group gap="xs">
        <Badge color={outcome.ok ? "green" : "red"} data-testid="syncing-last-run-badge">
          {outcome.ok
            ? t("syncingTab.publishComplete", { count: outcome.total_published })
            : t("syncingTab.publishPartial", {
                count: outcome.total_published,
                failed: outcome.errors.length,
              })}
        </Badge>
      </Group>
      {outcome.errors.length > 0 && (
        <Table data-testid="syncing-last-run-errors">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("syncingTab.errorAsset")}</Table.Th>
              <Table.Th>{t("syncingTab.errorMessage")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {outcome.errors.map((e) => (
              <Table.Tr key={`${e.asset}:${e.message}`}>
                <Table.Td>{e.asset}</Table.Td>
                <Table.Td>{e.message}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}

/** The differences an Import run applied or held, each with why it was held. */
function ImportItems({
  items,
  testId,
  withReason,
}: {
  items: ImportRunItem[];
  testId: string;
  withReason: boolean;
}) {
  const { t } = useTranslation();
  return (
    <Table data-testid={testId}>
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("syncingTab.importKind")}</Table.Th>
          <Table.Th>{t("syncingTab.importObject")}</Table.Th>
          {withReason && <Table.Th>{t("syncingTab.importReason")}</Table.Th>}
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {items.map((item) => (
          <Table.Tr key={`${item.kind}:${item.object}`}>
            <Table.Td>{t(`syncingTab.kind_${item.kind}`)}</Table.Td>
            <Table.Td>{item.object}</Table.Td>
            {withReason && <Table.Td>{item.reason}</Table.Td>}
          </Table.Tr>
        ))}
      </Table.Tbody>
    </Table>
  );
}

/** The entry's last Import run: when, how it was started, its outcome, what it applied and held. */
export function LastImport({ run }: { run: ImportRun }) {
  const { t, i18n } = useTranslation();
  const ok = run.outcome === "ok";
  return (
    <Stack gap={4} data-testid="syncing-last-import">
      <Title order={6}>{t("syncingTab.lastImport")}</Title>
      <Group gap="xs">
        <Badge color={ok ? "green" : "red"} data-testid="syncing-last-import-badge">
          {ok
            ? t("syncingTab.importOk", { applied: run.applied.length, held: run.held.length })
            : t("syncingTab.importFailed")}
        </Badge>
        <Text size="sm" c="dimmed" data-testid="syncing-last-import-when">
          {t(`syncingTab.importTrigger_${run.trigger}`, {
            when: new Date(run.started_at).toLocaleString(i18n.language),
          })}
        </Text>
      </Group>
      {run.error && (
        <Text size="sm" c="red" data-testid="syncing-last-import-error">
          {run.error}
        </Text>
      )}
      {run.applied.length > 0 && (
        <>
          <Text size="sm" fw={500}>
            {t("syncingTab.importApplied")}
          </Text>
          <ImportItems
            items={run.applied}
            testId="syncing-last-import-applied"
            withReason={false}
          />
        </>
      )}
      {run.held.length > 0 && (
        <>
          <Text size="sm" fw={500}>
            {t("syncingTab.importHeld")}
          </Text>
          <ImportItems items={run.held} testId="syncing-last-import-held" withReason />
        </>
      )}
    </Stack>
  );
}

type Shared = Omit<
  EntryFieldsProps,
  "entry" | "secrets" | "isNew" | "onChange" | "onSecret" | "entitled" | "importCronProblem"
>;

export function EntryPanel({
  entry,
  isNew,
  entitled,
  fields,
  onSave,
  onCancelNew,
  onDelete,
  onTest,
  onPublish,
  onRunImport,
  onDuplicate,
  note,
}: {
  entry: SyncEntry;
  isNew: boolean;
  entitled: boolean;
  fields: Shared;
  /** Keeps ``draft`` with the credentials typed for it; rejects with the reason it was refused. */
  onSave: (draft: SyncEntry, secrets: Secrets) => Promise<void>;
  onCancelNew: () => void;
  onDelete: () => Promise<void>;
  onTest: () => Promise<EntryHealth>;
  onPublish: () => Promise<PublishOutcome | undefined>;
  /** Runs the entry's Import once now; rejects with the reason it was refused or failed. */
  onRunImport: () => Promise<void>;
  /** Opens the create form filled from this entry. */
  onDuplicate: () => void;
  /** What the create form says above its fields: what a duplicate did not copy. */
  note?: string;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(isNew);
  const [draft, setDraft] = useState<SyncEntry>(entry);
  const [secrets, setSecrets] = useState<Secrets>({});
  const [problem, setProblem] = useState<string | null>(null);
  const [importCronProblem, setImportCronProblem] = useState<string | undefined>(undefined);
  const [busy, setBusy] = useState("");
  const [health, setHealth] = useState<EntryHealth | null>(null);

  const isCatalog = entry.type === "data_catalog";
  const exporting = isCatalog && exportsTo(entry.direction);
  const importing = isCatalog && importsFrom(entry.direction);

  if (editing) {
    const save = async () => {
      setProblem(null);
      setImportCronProblem(undefined);
      try {
        await onSave(draft, secrets);
        setSecrets({});
        setEditing(false);
      } catch (e) {
        // A refused import schedule of this entry is shown on its field; any other refusal above
        // the buttons.
        if (e instanceof SyncingRefused && e.field === "import_cron" && e.entry === draft.id) {
          setImportCronProblem(e.message);
          return;
        }
        setProblem(e instanceof Error ? e.message : String(e));
      }
    };
    return (
      <FormLayout data-testid="syncing-entry-form">
        {note && (
          <FormField size="full">
            <Alert
              color="yellow"
              variant="light"
              py={4}
              px="sm"
              data-testid="syncing-duplicate-note"
            >
              {note}
            </Alert>
          </FormField>
        )}
        <EntryFields
          {...fields}
          entry={draft}
          secrets={secrets}
          isNew={isNew}
          entitled={entitled}
          importCronProblem={importCronProblem}
          onChange={(p) => {
            if ("import_cron" in p) setImportCronProblem(undefined);
            setDraft((prev) => ({ ...prev, ...p }));
          }}
          onSecret={(field: SyncSecretField, value: string) =>
            setSecrets((prev) => ({ ...prev, [field]: value }))
          }
        />
        <FormFooter
          panel
          problem={problem ?? undefined}
          onSave={save}
          onCancel={() => {
            if (isNew) {
              onCancelNew();
              return;
            }
            setDraft(entry);
            setSecrets({});
            setProblem(null);
            setImportCronProblem(undefined);
            setEditing(false);
          }}
          saveProps={{
            "data-testid": "syncing-save",
            disabled: !draft.id.trim() || (draft.type === "data_catalog" && !draft.provider),
          }}
          cancelProps={{ "data-testid": "syncing-cancel" }}
        />
      </FormLayout>
    );
  }

  const act = async (name: string, run: () => Promise<void>) => {
    setBusy(name);
    setProblem(null);
    try {
      await run();
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  };

  return (
    <>
      <PanelActions>
        <fieldset className="panel-action-set">
          <Group gap="xs">
            {isCatalog && (
              <Button
                loading={busy === "test"}
                data-testid="syncing-test"
                onClick={() =>
                  act("test", async () => {
                    setHealth(await onTest());
                  })
                }
              >
                {t("syncingTab.testConnection")}
              </Button>
            )}
            {/* A direction that is off never runs: Publish now is offered only while it exports. */}
            {exporting && entitled && (
              <Button
                loading={busy === "publish"}
                data-testid="syncing-publish"
                onClick={() =>
                  act("publish", async () => {
                    await onPublish();
                  })
                }
              >
                {t("syncingTab.publishNow")}
              </Button>
            )}
            {/* A direction that is off never runs: Run import now is offered only while it
            imports. */}
            {importing && (
              <Button
                loading={busy === "import"}
                data-testid="syncing-run-import"
                onClick={() => act("import", onRunImport)}
              >
                {t("syncingTab.runImportNow")}
              </Button>
            )}
            <Tip text={t("syncingTab.edit")} named>
              <ActionIcon
                aria-label={t("syncingTab.edit")}
                data-testid="syncing-edit"
                onClick={() => {
                  setDraft(entry);
                  setEditing(true);
                }}
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
            <PanelDuplicate name={entry.id} onDuplicate={onDuplicate} testId="syncing-duplicate" />
            <PanelDelete
              itemKey={entry.id}
              title={t("syncingTab.deleteTitle", { id: entry.id })}
              consequence={t("syncingTab.deleteConsequence")}
              ariaLabel={t("syncingTab.delete")}
              testId="syncing-delete"
              onDelete={onDelete}
            />
          </Group>
        </fieldset>
      </PanelActions>
      <Stack gap="md">
        {problem && (
          <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="syncing-problem">
            {problem}
          </Alert>
        )}
        {health && (
          <Alert
            color={health.ok ? "green" : "red"}
            icon={health.ok ? <Check size={16} /> : <TriangleAlert size={16} />}
            data-testid="syncing-health"
          >
            {health.ok ? t("syncingTab.healthOk") : health.error}
          </Alert>
        )}
        <FormRead testId={`syncing-entry-read-${entry.id}`}>
          <EntryFields
            {...fields}
            entry={entry}
            secrets={{}}
            isNew={false}
            entitled={entitled}
            onChange={() => undefined}
            onSecret={() => undefined}
          />
        </FormRead>
        {isCatalog &&
          (entry.last_run ? (
            <LastRun outcome={entry.last_run} />
          ) : exporting ? (
            <Text size="sm" c="dimmed" data-testid="syncing-never-run">
              {t("syncingTab.neverRun")}
            </Text>
          ) : null)}
        {isCatalog &&
          (entry.last_import ? (
            <LastImport run={entry.last_import} />
          ) : importing ? (
            <Text size="sm" c="dimmed" data-testid="syncing-never-imported">
              {t("syncingTab.neverImported")}
            </Text>
          ) : null)}
        {/* The catalog is read only when the steward opens the comparison. */}
        {isCatalog && (
          <Collapsibles id="admin.syncing.entry">
            <CollapsibleItem
              value="compare"
              title={t("syncingTab.compare")}
              defaultOpen={false}
              mountWhenOpen
              testId="syncing-compare"
            >
              <EntrySync entryId={entry.id} />
            </CollapsibleItem>
          </Collapsibles>
        )}
      </Stack>
    </>
  );
}
