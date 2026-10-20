// Copyright (c) 2026 Kenneth Stott
// Canary: 42a83daa-1a22-4874-9059-f8ba460f2ccc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: the container registries a code source's built image is pushed to, entered once by an
// organisation administrator beside its package registries: a registry and repository namespace,
// a user name and a credential, optionally the PEM certificate authority its TLS chains to. One
// may be the default a build pushes to. Every field keeps what was typed -- the value, or a
// reference to a secret -- and shows it again; the credential behind the reveal control.

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
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  addContainerRegistry,
  deleteContainerRegistry,
  fetchContainerRegistries,
  replaceContainerRegistry,
  type ContainerRegistry,
} from "../../api/containerRegistries";
import { useAuth } from "../../context/AuthContext";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

/** An entry as the form holds it: a kept one (its id) or a new one (no id yet). */
interface Draft {
  key: string;
  id: string | null;
  address: string;
  username: string;
  credential: string;
  isDefault: boolean;
  caBundle: string;
}

const draftOf = (r: ContainerRegistry): Draft => ({
  key: r.id,
  id: r.id,
  address: r.address,
  username: r.username,
  credential: r.credential,
  isDefault: r.default,
  caBundle: r.ca_bundle ?? "",
});

let fresh = 0;
const blank = (): Draft => ({
  key: `new-${++fresh}`,
  id: null,
  address: "",
  username: "",
  credential: "",
  isDefault: false,
  caBundle: "",
});

/** The fields an entry cannot be kept without. */
const missing = (d: Draft): string[] =>
  [
    d.address.trim() ? null : "address",
    d.username.trim() ? null : "username",
    d.credential.trim() ? null : "credential",
  ].filter((f): f is string => f !== null);

export function ContainerRegistriesTab() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [drafts, setDrafts] = useState<Draft[] | null>(null);
  const [busy, setBusy] = useState("");
  const [said, setSaid] = useState<{ key: string; text: string; bad: boolean } | null>(null);
  const [error, setError] = useState("");

  const load = useCallback(() => {
    if (!activeOrgId) return;
    fetchContainerRegistries(activeOrgId)
      .then((kept) => setDrafts(kept.map(draftOf)))
      .catch((e: unknown) => setError(String(e)));
  }, [activeOrgId]);
  useEffect(load, [load]);

  if (error) {
    return (
      <Alert color="red" variant="light" data-testid="container-registries-error">
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
      address: d.address.trim(),
      username: d.username.trim(),
      credential: d.credential,
      default: d.isDefault,
      ca_bundle: d.caBundle.trim() || null,
    };
    return run(
      d.key,
      () =>
        d.id === null
          ? addContainerRegistry(activeOrgId, body)
          : replaceContainerRegistry(activeOrgId, d.id, body),
      t("containerRegistries.saved"),
    );
  };

  const remove = (d: Draft) =>
    d.id === null
      ? setDrafts((ds) => (ds ?? []).filter((x) => x.key !== d.key))
      : run(
          d.key,
          () => deleteContainerRegistry(activeOrgId, d.id as string),
          t("containerRegistries.removed"),
        );

  const entry = (d: Draft) => {
    const id = `container-registry-${d.key}`;
    return (
      <Card key={d.key} withBorder data-testid={id}>
        <FormLayout>
          <FormField size="long" help={t("containerRegistries.addressHelp")}>
            <TextInput
              label={t("containerRegistries.address")}
              placeholder="ghcr.io/example"
              required
              value={d.address}
              onChange={(e) => change(d.key, { address: e.currentTarget.value })}
              autoComplete="off"
              data-testid={`${id}-address`}
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              label={t("containerRegistries.username")}
              required
              value={d.username}
              onChange={(e) => change(d.key, { username: e.currentTarget.value })}
              autoComplete="off"
              data-testid={`${id}-username`}
            />
          </FormField>
          <FormField size="long" help={t("containerRegistries.credentialHelp")}>
            <PasswordInput
              label={t("containerRegistries.credential")}
              required
              value={d.credential}
              onChange={(e) => change(d.key, { credential: e.currentTarget.value })}
              autoComplete="new-password"
              data-testid={`${id}-credential`}
            />
          </FormField>
          <FormField size="long" help={t("containerRegistries.caBundleHelp")}>
            <Textarea
              label={t("containerRegistries.caBundle")}
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
          <FormChecks>
            <FormField size="short" help={t("containerRegistries.defaultHelp")}>
              <Checkbox
                label={t("containerRegistries.default")}
                checked={d.isDefault}
                onChange={(e) => change(d.key, { isDefault: e.currentTarget.checked })}
                data-testid={`${id}-default`}
              />
            </FormField>
          </FormChecks>
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
              {t("containerRegistries.remove")}
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

  return (
    <Stack gap="md" data-testid="container-registries">
      <Group justify="space-between" align="flex-start">
        <Text size="sm" style={{ flex: 1 }}>
          {t("containerRegistries.help")}
        </Text>
        <Button
          variant="default"
          onClick={() => setDrafts((ds) => [...(ds ?? []), blank()])}
          data-testid="container-registries-add"
        >
          {t("containerRegistries.add")}
        </Button>
      </Group>
      {drafts.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="container-registries-none">
          {t("containerRegistries.none")}
        </Text>
      ) : (
        drafts.map(entry)
      )}
    </Stack>
  );
}
