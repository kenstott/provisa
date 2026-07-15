// Copyright (c) 2026 Kenneth Stott
// Canary: d33e2c86-0e0d-4bf9-a3ae-c38b1225a0d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { FormEvent } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Box,
  Button,
  Group,
  List,
  Radio,
  Stack,
  Text,
  TextInput,
  PasswordInput,
  Title,
} from "@mantine/core";
import { runSetup } from "../api/setup";

interface SetupPageProps {
  onSetupComplete: () => void;
}

export function SetupPage({ onSetupComplete }: SetupPageProps) {
  const { t } = useTranslation();
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [mode, setMode] = useState<"single" | "multi">("single");
  const [provider, setProvider] = useState<"basic" | "firebase" | "none">("basic");
  const [adminUsername, setAdminUsername] = useState("admin");
  const [adminPassword, setAdminPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [firebaseProjectId, setFirebaseProjectId] = useState("");
  const [useDomains, setUseDomains] = useState<boolean | null>(false);
  const [defaultDomain, setDefaultDomain] = useState("default");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleFinish = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (provider === "basic" && adminPassword !== confirmPassword) {
      setError(t("setupPage.passwordMismatch"));
      return;
    }
    setLoading(true);
    try {
      await runSetup({
        provider,
        mode,
        admin_username: provider === "basic" ? adminUsername : undefined,
        admin_password: provider === "basic" ? adminPassword : undefined,
        firebase_project_id: provider === "firebase" ? firebaseProjectId : undefined,
        use_domains: useDomains,
        default_domain: defaultDomain,
      });
      onSetupComplete();
    } catch (err) {
      setError(err instanceof Error ? err.message : t("setupPage.setupFailed"));
    } finally {
      setLoading(false);
    }
  };

  const handleCompleteNoAuth = async () => {
    setError(null);
    setLoading(true);
    try {
      await runSetup({
        provider: "none",
        mode,
        use_domains: useDomains,
        default_domain: defaultDomain,
      });
      onSetupComplete();
    } catch (err) {
      setError(err instanceof Error ? err.message : t("setupPage.setupFailed"));
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box maw={480} mx="auto" my={80}>
      <Title order={2}>{t("setupPage.title")}</Title>
      <Text c="dimmed" size="sm" mb="lg">
        {t("setupPage.stepIndicator", { step })}
      </Text>

      {step === 1 && (
        <Stack gap="md">
          <Title order={3}>{t("setupPage.deploymentModeHeading")}</Title>
          <Radio.Group
            value={mode}
            onChange={(v) => setMode(v as "single" | "multi")}
            name="mode"
          >
            <Stack gap="md">
              <Radio
                value="single"
                data-testid="setup-mode-single"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.singleTenantLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.singleTenantDesc")}
                    </Text>
                  </Box>
                }
              />
              <Radio
                value="multi"
                data-testid="setup-mode-multi"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.multiTenantLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.multiTenantDesc")}
                    </Text>
                  </Box>
                }
              />
            </Stack>
          </Radio.Group>

          <Title order={3}>{t("setupPage.domainModelHeading")}</Title>
          <Radio.Group
            value={useDomains === false ? "simple" : useDomains === true ? "namespaced" : "legacy"}
            onChange={(v) => setUseDomains(v === "simple" ? false : v === "namespaced" ? true : null)}
            name="domains"
          >
            <Stack gap="md">
              <Radio
                value="simple"
                data-testid="setup-domains-simple"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.simpleDomainLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.simpleDomainDesc")}
                    </Text>
                  </Box>
                }
              />
              {useDomains === false && (
                <Box ml="lg">
                  <TextInput
                    id="setup-default-domain"
                    label={t("setupPage.defaultDomainNameLabel")}
                    value={defaultDomain}
                    onChange={(e) => setDefaultDomain(e.currentTarget.value)}
                    required
                    pattern="[A-Za-z_][A-Za-z0-9_]*"
                  />
                </Box>
              )}
              <Radio
                value="namespaced"
                data-testid="setup-domains-namespaced"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.namespacedDomainLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.namespacedDomainDesc")}
                    </Text>
                  </Box>
                }
              />
              <Radio
                value="legacy"
                data-testid="setup-domains-legacy"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.legacyDomainLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.legacyDomainDesc")}
                    </Text>
                  </Box>
                }
              />
            </Stack>
          </Radio.Group>

          <Group>
            <Button data-testid="setup-next-1" onClick={() => setStep(2)}>
              {t("setupPage.next")}
            </Button>
          </Group>
        </Stack>
      )}

      {step === 2 && (
        <Stack gap="md">
          <Title order={3}>{t("setupPage.identityProviderHeading")}</Title>
          <Radio.Group
            value={provider}
            onChange={(v) => setProvider(v as "basic" | "firebase" | "none")}
            name="provider"
          >
            <Stack gap="md">
              <Radio
                value="basic"
                data-testid="setup-provider-basic"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.basicAuthLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.basicAuthDesc")}
                    </Text>
                  </Box>
                }
              />
              <Radio
                value="firebase"
                data-testid="setup-provider-firebase"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.firebaseLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.firebaseDesc")}
                    </Text>
                  </Box>
                }
              />
              <Radio
                value="none"
                data-testid="setup-provider-none"
                label={
                  <Box>
                    <Text fw={700}>{t("setupPage.noneLabel")}</Text>
                    <Text size="sm" c="dimmed">
                      {t("setupPage.noneDesc")}
                    </Text>
                  </Box>
                }
              />
            </Stack>
          </Radio.Group>
          <Group>
            <Button variant="default" data-testid="setup-back-2" onClick={() => setStep(1)}>
              {t("setupPage.back")}
            </Button>
            {provider === "none" ? (
              <Button
                data-testid="setup-complete-noauth"
                loading={loading}
                onClick={handleCompleteNoAuth}
              >
                {loading ? t("setupPage.settingUp") : t("setupPage.completeSetup")}
              </Button>
            ) : (
              <Button data-testid="setup-next-2" onClick={() => setStep(3)}>
                {t("setupPage.next")}
              </Button>
            )}
          </Group>
        </Stack>
      )}

      {step === 3 && (
        <form onSubmit={handleFinish}>
          <Stack gap="md">
            {provider === "basic" && (
              <>
                <Title order={3}>{t("setupPage.adminAccountHeading")}</Title>
                <TextInput
                  id="setup-username"
                  label={t("setupPage.username")}
                  value={adminUsername}
                  onChange={(e) => setAdminUsername(e.currentTarget.value)}
                  required
                  autoComplete="username"
                />
                <PasswordInput
                  id="setup-password"
                  label={t("setupPage.password")}
                  value={adminPassword}
                  onChange={(e) => setAdminPassword(e.currentTarget.value)}
                  required
                  autoComplete="new-password"
                />
                <PasswordInput
                  id="setup-confirm"
                  label={t("setupPage.confirmPassword")}
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.currentTarget.value)}
                  required
                  autoComplete="new-password"
                />
              </>
            )}
            {provider === "firebase" && (
              <>
                <Title order={3}>{t("setupPage.firebaseConfigHeading")}</Title>
                <TextInput
                  id="setup-project-id"
                  label={t("setupPage.firebaseProjectId")}
                  value={firebaseProjectId}
                  onChange={(e) => setFirebaseProjectId(e.currentTarget.value)}
                  required
                  placeholder={t("setupPage.firebaseProjectIdPlaceholder")}
                />
                <Alert variant="light" color="gray">
                  <Text fw={700} size="sm">
                    {t("setupPage.requiredEnvVars")}
                  </Text>
                  <List size="sm" mt="xs">
                    <List.Item>VITE_FIREBASE_API_KEY</List.Item>
                    <List.Item>VITE_FIREBASE_AUTH_DOMAIN</List.Item>
                    <List.Item>VITE_FIREBASE_PROJECT_ID</List.Item>
                    <List.Item>FIREBASE_PROJECT_ID (backend)</List.Item>
                  </List>
                </Alert>
              </>
            )}
            {error && (
              <Alert variant="light" color="red" data-testid="setup-error">
                {error}
              </Alert>
            )}
            <Group>
              <Button type="button" variant="default" data-testid="setup-back-3" onClick={() => setStep(2)}>
                {t("setupPage.back")}
              </Button>
              <Button type="submit" data-testid="setup-submit" loading={loading}>
                {loading ? t("setupPage.settingUp") : t("setupPage.completeSetup")}
              </Button>
            </Group>
          </Stack>
        </form>
      )}
    </Box>
  );
}
