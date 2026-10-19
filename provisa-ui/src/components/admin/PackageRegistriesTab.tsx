// Copyright (c) 2026 Kenneth Stott
// Canary: 1327210d-927b-4de4-b9ce-418c2e447d3a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993: the package registries every Node and Python install of the organisation
// goes through, entered once by an organisation administrator: npm registries (an address, an
// optional scope, a credential; one may be the mirror), PyPI indexes (an address and a credential;
// one may be the default index) and Git hosts (a host and a credential). The public npm registry
// and the public PyPI need no entry.
//
// Every field keeps what was typed -- the value, or a reference to a secret the administrator
// created -- and shows it again; the credential behind the reveal control.
//
// REQ-2088: a credential may be user:password (npm, PyPI: an Artifactory user and its API key) or
// user:token (Git: GitLab, Bitbucket), and every entry may carry the PEM certificate authority its
// server's TLS certificate chains to, for a proxy or Git server behind a private CA.

import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Group,
  PasswordInput,
  Stack,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  addPackageRegistry,
  deletePackageRegistry,
  fetchPackageRegistries,
  replacePackageRegistry,
  type PackageRegistry,
  type RegistryKind,
} from "../../api/packageRegistries";
import { useAuth } from "../../context/AuthContext";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

/** An entry as the form holds it: a kept one (its id) or a new one (no id yet). */
interface Draft {
  key: string;
  id: string | null;
  kind: RegistryKind;
  address: string;
  scope: string;
  credential: string;
  mirror: boolean;
  caBundle: string;
}

const draftOf = (r: PackageRegistry): Draft => ({
  key: r.id,
  id: r.id,
  kind: r.kind,
  address: r.address,
  scope: r.scope ?? "",
  credential: r.credential,
  mirror: r.mirror,
  caBundle: r.ca_bundle ?? "",
});

let fresh = 0;
const blank = (kind: RegistryKind): Draft => ({
  key: `new-${kind}-${++fresh}`,
  id: null,
  kind,
  address: "",
  scope: "",
  credential: "",
  mirror: false,
  caBundle: "",
});

/** Each kind's own labels and placeholder; only npm has a scope, and Git hosts no mirror. */
const LABELS: Record<
  RegistryKind,
  {
    address: string;
    addressHelp: string | null;
    placeholder: string;
    credential: string;
    credentialHelp: string;
    mirror: string | null;
  }
> = {
  npm: {
    address: "packageRegistries.npmAddress",
    addressHelp: null,
    placeholder: "https://npm.example.com/",
    credential: "packageRegistries.npmCredential",
    credentialHelp: "packageRegistries.npmCredentialHelp",
    mirror: "packageRegistries.mirror",
  },
  pypi: {
    address: "packageRegistries.pypiAddress",
    addressHelp: null,
    placeholder: "https://pypi.example.com/simple/",
    credential: "packageRegistries.pypiCredential",
    credentialHelp: "packageRegistries.pypiCredentialHelp",
    mirror: "packageRegistries.pypiMirror",
  },
  git: {
    address: "packageRegistries.gitHost",
    addressHelp: "packageRegistries.gitHostHelp",
    placeholder: "git.example.com",
    credential: "packageRegistries.gitCredential",
    credentialHelp: "packageRegistries.gitCredentialHelp",
    mirror: null,
  },
};

/** The fields an entry cannot be kept without. */
const missing = (d: Draft): string[] =>
  [d.address.trim() ? null : "address", d.credential.trim() ? null : "credential"].filter(
    (f): f is string => f !== null,
  );

export function PackageRegistriesTab() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [drafts, setDrafts] = useState<Draft[] | null>(null);
  const [busy, setBusy] = useState("");
  const [said, setSaid] = useState<{ key: string; text: string; bad: boolean } | null>(null);
  const [error, setError] = useState("");

  const load = useCallback(() => {
    if (!activeOrgId) return;
    fetchPackageRegistries(activeOrgId)
      .then((kept) => setDrafts(kept.map(draftOf)))
      .catch((e: unknown) => setError(String(e)));
  }, [activeOrgId]);
  useEffect(load, [load]);

  if (error) {
    return (
      <Alert color="red" variant="light" data-testid="package-registries-error">
        {error}
      </Alert>
    );
  }
  if (!drafts || !activeOrgId) return null;

  const change = (key: string, patch: Partial<Draft>) =>
    setDrafts((ds) => (ds ?? []).map((d) => (d.key === key ? { ...d, ...patch } : d)));

  const run = async (key: string, act: () => Promise<unknown>, done: string) => {
    setBusy(key);
    setSaid(null);
    try {
      await act();
      setSaid({ key, text: done, bad: false });
      load();
    } catch (e) {
      setSaid({ key, text: String(e), bad: true });
    } finally {
      setBusy("");
    }
  };

  const save = (d: Draft) => {
    const body = {
      kind: d.kind,
      address: d.address.trim(),
      scope: d.kind === "npm" ? d.scope.trim() || null : null,
      credential: d.credential,
      mirror: LABELS[d.kind].mirror !== null && d.mirror,
      ca_bundle: d.caBundle.trim() || null,
    };
    return run(
      d.key,
      () =>
        d.id === null
          ? addPackageRegistry(activeOrgId, body)
          : replacePackageRegistry(activeOrgId, d.id, body),
      t("packageRegistries.saved"),
    );
  };

  const remove = (d: Draft) =>
    d.id === null
      ? setDrafts((ds) => (ds ?? []).filter((x) => x.key !== d.key))
      : run(
          d.key,
          () => deletePackageRegistry(activeOrgId, d.id as string),
          t("packageRegistries.removed"),
        );

  const entry = (d: Draft) => {
    const id = `package-registry-${d.key}`;
    const labels = LABELS[d.kind];
    return (
      <Card key={d.key} withBorder data-testid={id}>
        <FormLayout>
          <FormField size="long" help={labels.addressHelp ? t(labels.addressHelp) : undefined}>
            <TextInput
              label={t(labels.address)}
              placeholder={labels.placeholder}
              required
              value={d.address}
              onChange={(e) => change(d.key, { address: e.currentTarget.value })}
              autoComplete="off"
              data-testid={`${id}-address`}
            />
          </FormField>
          {d.kind === "npm" && (
            <FormField size="short" help={t("packageRegistries.scopeHelp")}>
              <TextInput
                label={t("packageRegistries.scope")}
                placeholder="@example"
                value={d.scope}
                onChange={(e) => change(d.key, { scope: e.currentTarget.value })}
                autoComplete="off"
                data-testid={`${id}-scope`}
              />
            </FormField>
          )}
          <FormField size="long" help={t(labels.credentialHelp)}>
            <PasswordInput
              label={t(labels.credential)}
              required
              value={d.credential}
              onChange={(e) => change(d.key, { credential: e.currentTarget.value })}
              autoComplete="new-password"
              data-testid={`${id}-credential`}
            />
          </FormField>
          <FormField size="long" help={t("packageRegistries.caBundleHelp")}>
            <Textarea
              label={t("packageRegistries.caBundle")}
              placeholder={"-----BEGIN CERTIFICATE-----"}
              value={d.caBundle}
              onChange={(e) => change(d.key, { caBundle: e.currentTarget.value })}
              autosize
              minRows={2}
              maxRows={8}
              styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
              data-testid={`${id}-ca-bundle`}
            />
          </FormField>
          {labels.mirror !== null && (
            <FormChecks>
              <FormField size="short" help={t(`${labels.mirror}Help`)}>
                <Checkbox
                  label={t(labels.mirror)}
                  checked={d.mirror}
                  onChange={(e) => change(d.key, { mirror: e.currentTarget.checked })}
                  data-testid={`${id}-mirror`}
                />
              </FormField>
            </FormChecks>
          )}
          <FormFooter
            onSave={() => save(d)}
            saving={busy === d.key}
            saveProps={{ "data-testid": `${id}-save`, disabled: missing(d).length > 0 }}
          >
            <Button
              variant="default"
              color="red"
              onClick={() => remove(d)}
              disabled={busy === d.key}
              data-testid={`${id}-remove`}
            >
              {t("packageRegistries.remove")}
            </Button>
            {said?.key === d.key && (
              <Text size="sm" c={said.bad ? "red" : "green"} data-testid={`${id}-said`}>
                {said.text}
              </Text>
            )}
          </FormFooter>
        </FormLayout>
      </Card>
    );
  };

  const section = (kind: RegistryKind) => {
    const listed = drafts.filter((d) => d.kind === kind);
    return (
      <Stack gap="sm" data-testid={`package-registries-${kind}`}>
        <Group justify="space-between">
          <div>
            <Title order={4}>{t(`packageRegistries.${kind}Title`)}</Title>
            <Text size="sm" c="dimmed">
              {t(`packageRegistries.${kind}Help`)}
            </Text>
          </div>
          <Button
            variant="default"
            onClick={() => setDrafts((ds) => [...(ds ?? []), blank(kind)])}
            data-testid={`package-registries-add-${kind}`}
          >
            {t(`packageRegistries.add_${kind}`)}
          </Button>
        </Group>
        {listed.length === 0 ? (
          <Text size="sm" c="dimmed" data-testid={`package-registries-${kind}-none`}>
            {t(`packageRegistries.${kind}None`)}
          </Text>
        ) : (
          listed.map(entry)
        )}
      </Stack>
    );
  };

  return (
    <Stack gap="xl" data-testid="package-registries">
      <Text size="sm">{t("packageRegistries.help")}</Text>
      {section("npm")}
      {section("pypi")}
      {section("git")}
    </Stack>
  );
}
