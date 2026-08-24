// Copyright (c) 2026 Kenneth Stott
// Canary: 7d2b91af-6e34-4c05-b8d1-52a9f3c67e40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Card,
  Group,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { missingRequired, secretPlaceholder, seedFields } from "./secretFields";
import {
  fetchMail,
  fetchMailStats,
  sendTestMail,
  setMail,
  type MailEvent,
  type MailState,
  type MailStats,
} from "../../api/mail";

/**
 * REQ-1576: the deployment's mail transport, owned by the platform_admin.
 *
 * An invitation is a platform communication, so the mail server behind it is a platform setting —
 * set here rather than by editing provisa.yaml on the node. Every transport the build knows is
 * offered, installed or not: an operator asking "can Provisa send through SES?" is answered by the
 * row being there with the missing package named, not by an absence that reads as "no".
 *
 * Below the form is the only answer that matters afterwards — whether mail is actually going out —
 * read from the record of real attempts rather than from the configuration that produced them.
 *
 * REQ-1575: a credential typed here never comes back out. A secret field starts empty; leaving it
 * empty keeps what is stored, and it is sent only when it is typed into.
 */
export function MailTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<MailState | null>(null);
  const [stats, setStats] = useState<MailStats | null>(null);
  const [choice, setChoice] = useState("");
  const [config, setConfig] = useState<Record<string, Record<string, string>>>({});
  const [shared, setShared] = useState({ from_address: "", base_url: "", timeout_seconds: 10 });
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [testTo, setTestTo] = useState("");
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; error?: string } | null>(null);

  const load = () =>
    fetchMail()
      .then((state) => {
        setS(state);
        setChoice(state.provider);
        setShared({
          from_address: state.from_address,
          base_url: state.base_url,
          timeout_seconds: state.timeout_seconds,
        });
        // REQ-1575: seeded from what the server sent, which is every field EXCEPT the secret ones.
        setConfig(
          Object.fromEntries(
            state.providers.map((p) => [p.key, seedFields(p.config_fields, state.config[p.key])]),
          ),
        );
      })
      .catch((e) => setError(String(e)));

  const loadStats = () =>
    fetchMailStats()
      .then(setStats)
      .catch((e) => setError(String(e)));

  useEffect(() => {
    load();
    loadStats();
  }, []);

  const selected = useMemo(() => s?.providers.find((p) => p.key === choice), [s, choice]);
  // A stored credential satisfies its required field without being retyped (REQ-1575).
  const incomplete = missingRequired(
    selected?.config_fields ?? [],
    config[choice],
    s?.secret_set?.[choice],
  );

  const setField = (key: string, value: string) =>
    setConfig((c) => ({ ...c, [choice]: { ...(c[choice] ?? {}), [key]: value } }));

  const save = async () => {
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setMail({ provider: choice, ...shared, config: config[choice] ?? {} });
      setMsg(t("mailTab.saved", { provider: res.provider }));
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const test = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      setTestResult(await sendTestMail(testTo));
      loadStats();
    } catch (e) {
      setError(String(e));
    } finally {
      setTesting(false);
    }
  };

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s) return <Text>{t("mailTab.loading")}</Text>;

  return (
    <Stack gap="lg" maw={820} data-testid="mail-tab">
      <Stack gap="sm">
        <Text c="dimmed" fz="sm">
          {t("mailTab.intro")}
        </Text>

        {error && <Alert color="red">{error}</Alert>}
        {msg && (
          <Alert color="green" data-testid="mail-saved">
            {msg}
          </Alert>
        )}

        <Select
          label={t("mailTab.providerLabel")}
          description={selected?.description}
          value={choice}
          onChange={(v) => setChoice(v ?? "")}
          allowDeselect={false}
          data={s.providers.map((p) => ({
            value: p.key,
            // An uninstalled transport stays on the list, disabled, naming what it needs.
            label: p.available
              ? p.label
              : t("mailTab.providerRequires", { label: p.label, library: p.requires }),
            disabled: !p.available,
          }))}
          data-testid="mail-provider"
        />

        {(selected?.config_fields ?? []).map((f) => (
          <TextInput
            key={f.config_key}
            label={f.label}
            required={f.required}
            type={f.type === "number" ? "number" : "text"}
            placeholder={
              secretPlaceholder(f, s.secret_set?.[choice], {
                set: t("mailTab.secretOnFile"),
                unset: t("mailTab.secretNotSet"),
              }) ?? f.placeholder
            }
            value={config[choice]?.[f.config_key] ?? ""}
            onChange={(e) => setField(f.config_key, e.currentTarget.value)}
            data-testid={`mail-field-${f.config_key}`}
          />
        ))}

        <TextInput
          label={t("mailTab.fromAddressLabel")}
          description={t("mailTab.fromAddressHelp")}
          value={shared.from_address}
          onChange={(e) => setShared({ ...shared, from_address: e.currentTarget.value })}
          data-testid="mail-from-address"
        />
        <TextInput
          label={t("mailTab.baseUrlLabel")}
          description={t("mailTab.baseUrlHelp")}
          value={shared.base_url}
          onChange={(e) => setShared({ ...shared, base_url: e.currentTarget.value })}
          data-testid="mail-base-url"
        />
        <NumberInput
          label={t("mailTab.timeoutLabel")}
          min={1}
          value={shared.timeout_seconds}
          onChange={(v) => setShared({ ...shared, timeout_seconds: Number(v) || 1 })}
          data-testid="mail-timeout"
        />

        <Group>
          <Button
            onClick={save}
            loading={saving}
            disabled={incomplete || choice === ""}
            data-testid="mail-save"
          >
            {t("mailTab.save")}
          </Button>
        </Group>
      </Stack>

      <Card withBorder padding="md" data-testid="mail-test">
        <Stack gap="sm">
          <Title order={5}>{t("mailTab.testTitle")}</Title>
          <Text c="dimmed" fz="sm">
            {t("mailTab.testIntro")}
          </Text>
          <Group align="flex-end">
            <TextInput
              label={t("mailTab.testRecipient")}
              value={testTo}
              onChange={(e) => setTestTo(e.currentTarget.value)}
              w={320}
              data-testid="mail-test-recipient"
            />
            <Button
              onClick={test}
              loading={testing}
              disabled={!testTo.trim()}
              data-testid="mail-test-send"
            >
              {t("mailTab.testSend")}
            </Button>
          </Group>
          {testResult && (
            <Alert
              color={testResult.success ? "green" : "red"}
              data-testid="mail-test-result"
              // The transport's own words, verbatim: "550 sender domain not verified" is the
              // answer, and a paraphrase of it is not.
            >
              {testResult.success ? t("mailTab.testSucceeded", { to: testTo }) : testResult.error}
            </Alert>
          )}
        </Stack>
      </Card>

      <MailStatsPanel stats={stats} />
    </Stack>
  );
}

/** What the transport has actually done — counts, the last success, the last failure (REQ-1576). */
function MailStatsPanel({ stats }: { stats: MailStats | null }) {
  const { t } = useTranslation();
  if (!stats) return <Text>{t("mailTab.statsLoading")}</Text>;
  const windows: [string, { attempted: number; delivered: number; failed: number }][] = [
    [t("mailTab.windowDay"), stats.windows.day],
    [t("mailTab.windowWeek"), stats.windows.week],
    [t("mailTab.windowTotal"), stats.total],
  ];
  return (
    <Card withBorder padding="md" data-testid="mail-stats">
      <Stack gap="sm">
        <Title order={5}>{t("mailTab.statsTitle")}</Title>
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("mailTab.window")}</Table.Th>
              <Table.Th>{t("mailTab.attempted")}</Table.Th>
              <Table.Th>{t("mailTab.delivered")}</Table.Th>
              <Table.Th>{t("mailTab.failed")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {windows.map(([label, w]) => (
              <Table.Tr key={label} data-testid={`mail-window-${label}`}>
                <Table.Td>{label}</Table.Td>
                <Table.Td>{w.attempted}</Table.Td>
                <Table.Td>{w.delivered}</Table.Td>
                <Table.Td>{w.failed}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>

        <Group gap="xl" align="flex-start">
          <Stack gap={2} data-testid="mail-last-success">
            <Text fw={500} fz="sm">
              {t("mailTab.lastSuccess")}
            </Text>
            <Text fz="sm" c="dimmed">
              {stats.last_success
                ? t("mailTab.eventLine", {
                    when: stats.last_success.sent_at,
                    to: stats.last_success.recipient,
                  })
                : t("mailTab.never")}
            </Text>
          </Stack>
          <Stack gap={2} data-testid="mail-last-failure">
            <Text fw={500} fz="sm">
              {t("mailTab.lastFailure")}
            </Text>
            <Text fz="sm" c="dimmed">
              {stats.last_failure
                ? t("mailTab.eventLine", {
                    when: stats.last_failure.sent_at,
                    to: stats.last_failure.recipient,
                  })
                : t("mailTab.never")}
            </Text>
            {stats.last_failure?.error && (
              <Text fz="sm" c="red" data-testid="mail-last-failure-error">
                {stats.last_failure.error}
              </Text>
            )}
          </Stack>
        </Group>

        {stats.recent.length > 0 && (
          <>
            <Text fw={500} fz="sm">
              {t("mailTab.recentTitle")}
            </Text>
            <Table data-testid="mail-recent">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("mailTab.when")}</Table.Th>
                  <Table.Th>{t("mailTab.kind")}</Table.Th>
                  <Table.Th>{t("mailTab.recipient")}</Table.Th>
                  <Table.Th>{t("mailTab.transport")}</Table.Th>
                  <Table.Th>{t("mailTab.outcome")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {stats.recent.map((e: MailEvent, i: number) => (
                  <Table.Tr key={`${e.sent_at}-${i}`}>
                    <Table.Td>{e.sent_at}</Table.Td>
                    <Table.Td>{e.kind}</Table.Td>
                    <Table.Td>{e.recipient}</Table.Td>
                    <Table.Td>{e.provider}</Table.Td>
                    <Table.Td>
                      {e.succeeded ? (
                        <Badge color="green" variant="light">
                          {t("mailTab.delivered")}
                        </Badge>
                      ) : (
                        <Group gap="xs">
                          <Badge color="red" variant="light">
                            {t("mailTab.failed")}
                          </Badge>
                          <Text fz="xs" c="dimmed">
                            {e.error}
                          </Text>
                        </Group>
                      )}
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </>
        )}
      </Stack>
    </Card>
  );
}
