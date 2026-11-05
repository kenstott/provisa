// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: Admin > Federation when the org uses regions -- one tab per region, each its engine
// store as the platform assigned it to the org: the engine kind and the URL exactly as entered,
// its password never shown. The platform administrator edits it and saves the org's whole grant;
// everyone else, a demo sandbox's visitor among them, reads it.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Code, Select, Stack, Tabs, Text, TextInput } from "@mantine/core";
import { Info, Pencil } from "lucide-react";
import { problemOf, type Grant, type GrantView, type OrgRegion, type Problem } from "../../api/regions";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { PanelActions } from "../list/SplitPane";
import { Tip } from "../Tip";

export function RegionEngineTabs({
  grant,
  save,
  sandbox,
}: {
  grant: GrantView;
  save: ((grant: Grant) => Promise<void>) | null;
  sandbox: boolean;
}) {
  const { t } = useTranslation();
  const [saved, setSaved] = useState(false);
  return (
    <Stack gap="md" data-testid="federation-regions">
      <Text c="dimmed" size="sm" maw={820}>
        {t("federationRegions.intro")}
      </Text>
      {save === null ? (
        <Alert color="blue" icon={<Info size={16} />} data-testid="federation-regions-read-only">
          {sandbox ? t("federationRegions.sandboxNote") : t("federationRegions.readOnlyNote")}
        </Alert>
      ) : (
        <Alert
          color={saved ? "orange" : "blue"}
          icon={<Info size={16} />}
          data-testid="federation-regions-restart"
        >
          {saved ? t("federationRegions.restartPending") : t("federationRegions.restartNote")}
        </Alert>
      )}
      <Tabs defaultValue={grant.regions[0].id} keepMounted={false}>
        <Tabs.List mb="md">
          {grant.regions.map((r) => (
            <Tabs.Tab key={r.id} value={r.id} data-testid={`federation-region-tab-${r.id}`}>
              {r.id}
            </Tabs.Tab>
          ))}
        </Tabs.List>
        {grant.regions.map((r) => (
          <Tabs.Panel key={r.id} value={r.id}>
            <RegionEngine
              grant={grant}
              region={r}
              save={
                save === null
                  ? null
                  : async (next) => {
                      await save(next);
                      setSaved(true);
                    }
              }
            />
          </Tabs.Panel>
        ))}
      </Tabs>
    </Stack>
  );
}

/** One region's engine store, read, or edited by the platform administrator. */
function RegionEngine({
  grant,
  region,
  save,
}: {
  grant: GrantView;
  region: OrgRegion;
  save: ((grant: Grant) => Promise<void>) | null;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const store = grant.stores.find((s) => s.id === region.engine);
  // A store two regions name as their engine is one store: a change here is a change there too.
  const sharedWith = grant.regions
    .filter((r) => r.id !== region.id && r.engine === region.engine)
    .map((r) => r.id);
  const testId = `federation-region-${region.id}`;

  if (store === undefined) {
    return (
      <Alert color="red" data-testid={`${testId}-missing`}>
        {t("federationRegions.storeMissing", { region: region.id, store: region.engine })}
      </Alert>
    );
  }
  if (editing && save !== null) {
    return (
      <EngineForm
        storeId={store.id}
        kind={store.kind}
        url={store.url}
        kinds={grant.engine_kinds}
        sharedWith={sharedWith}
        testId={testId}
        onCancel={() => setEditing(false)}
        onSave={async (kind, url) => {
          await save({
            stores: grant.stores.map((s) => (s.id === store.id ? { ...s, kind, url } : s)),
            regions: grant.regions,
          });
          setEditing(false);
        }}
      />
    );
  }
  return (
    <Stack gap="sm">
      {save !== null && (
        <PanelActions>
          <Tip text={t("federationRegions.edit")} named>
            <ActionIcon
              aria-label={t("federationRegions.edit")}
              data-testid={`${testId}-edit`}
              onClick={() => setEditing(true)}
            >
              <Pencil size={14} />
            </ActionIcon>
          </Tip>
        </PanelActions>
      )}
      <DetailFields
        testId={`${testId}-read`}
        fields={[
          { label: t("federationRegions.store"), value: store.id },
          {
            label: t("federationRegions.kind"),
            value: store.kind,
            testId: `${testId}-kind`,
          },
          {
            label: t("federationRegions.url"),
            value: <Code>{store.url}</Code>,
            testId: `${testId}-url`,
          },
          {
            label: t("federationRegions.sharedWith"),
            value: sharedWith.join(", "),
          },
        ]}
      />
    </Stack>
  );
}

function EngineForm({
  storeId,
  kind: givenKind,
  url: givenUrl,
  kinds,
  sharedWith,
  testId,
  onSave,
  onCancel,
}: {
  storeId: string;
  kind: string | null;
  url: string;
  kinds: string[];
  sharedWith: string[];
  testId: string;
  onSave: (kind: string, url: string) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [kind, setKind] = useState<string | null>(givenKind);
  const [url, setUrl] = useState(givenUrl);
  const [problem, setProblem] = useState<Problem | null>(null);
  const [saving, setSaving] = useState(false);
  // The field a refusal names, when it is this store's: `stores.eu-trino.url` → `url`.
  const prefix = `stores.${storeId}.`;
  const marked = problem?.field?.startsWith(prefix) ? problem.field.slice(prefix.length) : null;

  const submit = async () => {
    if (kind === null) return;
    setProblem(null);
    setSaving(true);
    try {
      // The URL as entered: a reference stays the reference, and an address sent back without its
      // password keeps the one held.
      await onSave(kind, url);
    } catch (e) {
      setProblem(problemOf(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <FormLayout data-testid={`${testId}-form`}>
      <FormIdentifiers>
        <FormField
          size="medium"
          help={t("federationRegions.storeHelp")}
          hint={
            sharedWith.length
              ? t("federationRegions.sharedHint", { regions: sharedWith.join(", ") })
              : undefined
          }
        >
          <TextInput label={t("federationRegions.store")} value={storeId} disabled />
        </FormField>
      </FormIdentifiers>
      <FormField size="medium" help={t("federationRegions.kindHelp")}>
        <Select
          label={t("federationRegions.kind")}
          required
          data={kinds}
          value={kind}
          onChange={setKind}
          allowDeselect={false}
          error={marked === "kind" ? true : undefined}
          comboboxProps={{ withinPortal: true }}
          data-testid={`${testId}-kind-input`}
        />
      </FormField>
      <FormField size="long" help={t("federationRegions.urlHelp")}>
        <TextInput
          label={t("federationRegions.url")}
          required
          value={url}
          onChange={(e) => setUrl(e.currentTarget.value)}
          error={marked === "url" ? true : undefined}
          autoComplete="off"
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          data-testid={`${testId}-url-input`}
        />
      </FormField>
      <FormFooter
        problem={problem?.text}
        saving={saving}
        onSave={submit}
        onCancel={onCancel}
        saveProps={{
          "data-testid": `${testId}-save`,
          disabled: kind === null || !url.trim(),
        }}
      />
    </FormLayout>
  );
}
