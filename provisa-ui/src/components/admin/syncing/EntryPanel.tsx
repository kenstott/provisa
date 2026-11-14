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
// in the panel's head. A type with no run of its own yet is offered none.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Badge, Button, Group, Stack, Table, Text, Title } from "@mantine/core";
import { Check, Pencil, TriangleAlert } from "lucide-react";
import {
  exportsTo,
  type EntryHealth,
  type PublishOutcome,
  type SyncEntry,
  type SyncSecretField,
} from "../../../api/syncing";
import { FormFooter, FormLayout, FormRead } from "../../form/FormLayout";
import { PanelActions } from "../../list/SplitPane";
import { PanelDelete } from "../../list/PanelDelete";
import { Tip } from "../../Tip";
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

type Shared = Omit<
  EntryFieldsProps,
  "entry" | "secrets" | "isNew" | "onChange" | "onSecret" | "entitled"
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
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(isNew);
  const [draft, setDraft] = useState<SyncEntry>(entry);
  const [secrets, setSecrets] = useState<Secrets>({});
  const [problem, setProblem] = useState<string | null>(null);
  const [busy, setBusy] = useState("");
  const [health, setHealth] = useState<EntryHealth | null>(null);

  const isCatalog = entry.type === "data_catalog";
  const exporting = isCatalog && exportsTo(entry.direction);

  if (editing) {
    const save = async () => {
      setProblem(null);
      try {
        await onSave(draft, secrets);
        setSecrets({});
        setEditing(false);
      } catch (e) {
        setProblem(e instanceof Error ? e.message : String(e));
      }
    };
    return (
      <FormLayout data-testid="syncing-entry-form">
        <EntryFields
          {...fields}
          entry={draft}
          secrets={secrets}
          isNew={isNew}
          entitled={entitled}
          onChange={(p) => setDraft((prev) => ({ ...prev, ...p }))}
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
                variant="subtle"
                size="compact-sm"
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
                variant="subtle"
                size="compact-sm"
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
            <Tip text={t("syncingTab.edit")} named>
              <ActionIcon
                variant="subtle"
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
      </Stack>
    </>
  );
}
