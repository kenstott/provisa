// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, Select, Stack, Text, TextInput } from "@mantine/core";
import { Check } from "lucide-react";
import {
  fetchEncryption,
  setEncryption,
  generateEncryptionKey,
  type EncryptionState,
} from "../../api/admin";

// REQ-918: manage the encryption provider + master key. The provider binds at startup, so provider
// changes take effect on restart; generating a key stores it in the OS keychain immediately.
export function EncryptionTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<EncryptionState | null>(null);
  const [provider, setProvider] = useState("null");
  const [keyId, setKeyId] = useState("");
  const [saving, setSaving] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [generatedKey, setGeneratedKey] = useState<string | null>(null);

  const load = () =>
    fetchEncryption()
      .then((e) => {
        setS(e);
        setProvider(e.provider);
        setKeyId(e.key_id ?? "");
      })
      .catch((e) => setError(String(e)));

  useEffect(() => {
    load();
  }, []);

  const current = useMemo(() => s?.providers.find((p) => p.key === provider), [s, provider]);

  const save = async () => {
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setEncryption({ provider, key_id: keyId || null });
      setMsg(res.restart_required ? t("encryptionTab.savedRestartRequired") : t("encryptionTab.saved"));
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const generate = async () => {
    setGenerating(true);
    setMsg("");
    setError("");
    setGeneratedKey(null);
    try {
      const res = await generateEncryptionKey({ key_id: keyId || null });
      if (res.stored) {
        setMsg(t("encryptionTab.keyGeneratedStored", { keyId: res.key_id }));
      } else {
        setGeneratedKey(res.key_b64);
        setMsg(t("encryptionTab.keyGeneratedNoKeychain", { envVar: res.env_var }));
      }
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setGenerating(false);
    }
  };

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s) return <Text>{t("encryptionTab.loading")}</Text>;

  const providerData = s.providers.map((p) => ({
    value: p.key,
    label: p.label + (p.key === s.provider ? t("encryptionTab.providerCurrentSuffix") : ""),
  }));

  return (
    <Stack gap="md" maw={720}>
      <Text c="dimmed">{t("encryptionTab.intro")}</Text>

      <Stack gap="sm">
        <Select
          label={t("encryptionTab.providerLabel")}
          data={providerData}
          value={provider}
          onChange={(v) => v && setProvider(v)}
          allowDeselect={false}
          data-testid="encryption-provider-select"
        />
        {current && <Text c="dimmed" fz="xs">{current.description}</Text>}

        {provider === "local" && (
          <>
            <TextInput
              label={t("encryptionTab.keyIdLabel")}
              placeholder={t("encryptionTab.keyIdPlaceholder")}
              value={keyId}
              onChange={(e) => setKeyId(e.currentTarget.value)}
            />

            <Group gap="sm" align="center">
              <Text>
                {t("encryptionTab.masterKeyLabel")}{" "}
                {s.key_present ? (
                  <Badge color="green" variant="light">
                    {t("encryptionTab.masterKeyPresent")}
                  </Badge>
                ) : (
                  <Badge color="red" variant="light">
                    {t("encryptionTab.masterKeyMissing")}
                  </Badge>
                )}
              </Text>
              <Button
                variant="default"
                onClick={generate}
                loading={generating}
                data-testid="generate-key-button"
              >
                {s.key_present ? t("encryptionTab.rotateKey") : t("encryptionTab.generateKey")}
              </Button>
            </Group>

            {generatedKey && (
              <Alert color="red" variant="outline">
                <Text ff="monospace" fz="sm" style={{ wordBreak: "break-all" }}>
                  {generatedKey}
                </Text>
              </Alert>
            )}
          </>
        )}
      </Stack>

      <Alert color="yellow" variant="light">
        {t("encryptionTab.restartWarning", { note: s.restart_required_note })}
      </Alert>

      <Group gap="sm" align="center">
        <Button
          onClick={save}
          loading={saving}
          title={t("encryptionTab.saveTitle")}
          aria-label={t("encryptionTab.saveTitle")}
          data-testid="save-encryption-button"
        >
          <Check size={14} />
        </Button>
        {msg && <Text c="green">{msg}</Text>}
        {error && <Text c="red">{error}</Text>}
      </Group>
    </Stack>
  );
}
