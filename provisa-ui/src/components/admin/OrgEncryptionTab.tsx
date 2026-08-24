// Copyright (c) 2026 Kenneth Stott
// Canary: 2232db08-4fdf-4cc9-9de7-d02d094e1b44
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, PasswordInput, Radio, Stack, Text } from "@mantine/core";
import { fetchOrgEncryption, setOrgEncryption, type OrgEncryptionState } from "../../api/admin";

/**
 * REQ-1574: the org's own encryption key — set it, rotate it, never see it.
 *
 * There is no reveal control and no copy button, because there is nothing to reveal: the server
 * returns a FINGERPRINT and never key material, on the first set as much as on the hundredth read.
 * A generated key therefore exists nowhere but the ring, which is the whole point — a key the
 * platform can hand back is a key the platform can hand to somebody else.
 *
 * Rotation is immediate and is NOT a re-encryption: the previous key is retired into the ring and
 * goes on decrypting what it wrote, so nothing already stored becomes unreadable. That also means
 * rotation is not revocation, which is said plainly on the surface rather than left to be found out.
 */
export function OrgEncryptionTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<OrgEncryptionState | null>(null);
  const [mode, setMode] = useState<"generate" | "supply">("generate");
  const [keyB64, setKeyB64] = useState("");
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  const load = () =>
    fetchOrgEncryption()
      .then(setS)
      .catch((e) => setError(String(e)));

  useEffect(() => {
    load();
  }, []);

  const apply = async () => {
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setOrgEncryption({ key_b64: mode === "supply" ? keyB64.trim() : null });
      setKeyB64("");
      setMsg(t("orgEncryptionTab.applied", { fingerprint: res.fingerprint, keyId: res.key_id }));
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s) return <Text>{t("orgEncryptionTab.loading")}</Text>;

  return (
    <Stack gap="md" maw={720} data-testid="org-encryption">
      <Text c="dimmed" size="sm">
        {t("orgEncryptionTab.intro")}
      </Text>

      {s.configured ? (
        <Stack gap="xs" data-testid="org-encryption-status">
          <Group gap="sm">
            <Text fw={600}>{t("orgEncryptionTab.fingerprintLabel")}</Text>
            <Text ff="monospace" data-testid="org-encryption-fingerprint">
              {s.fingerprint}
            </Text>
            <Badge variant="light">{s.key_id}</Badge>
            <Badge variant="light" color={s.supplied ? "blue" : "grape"}>
              {s.supplied ? t("orgEncryptionTab.supplied") : t("orgEncryptionTab.generated")}
            </Badge>
          </Group>
          <Text size="xs" c="dimmed">
            {t("orgEncryptionTab.setOn", {
              when: s.created_at ?? "—",
              actor: s.created_by ?? "—",
            })}
          </Text>
          {s.retired_count !== undefined && s.retired_count > 0 && (
            <Text size="xs" c="dimmed" data-testid="org-encryption-retired">
              {t("orgEncryptionTab.retired", { count: s.retired_count })}
            </Text>
          )}
        </Stack>
      ) : (
        <Alert color="blue" variant="light" data-testid="org-encryption-unset">
          {t("orgEncryptionTab.unset")}
        </Alert>
      )}

      <Radio.Group
        value={mode}
        onChange={(v) => setMode(v as "generate" | "supply")}
        label={t("orgEncryptionTab.modeLabel")}
      >
        <Stack gap="xs" mt="xs">
          <Radio
            value="generate"
            label={t("orgEncryptionTab.modeGenerate")}
            data-testid="org-encryption-mode-generate"
          />
          <Radio
            value="supply"
            label={t("orgEncryptionTab.modeSupply")}
            data-testid="org-encryption-mode-supply"
          />
        </Stack>
      </Radio.Group>

      {mode === "supply" && (
        <PasswordInput
          label={t("orgEncryptionTab.keyLabel")}
          description={t("orgEncryptionTab.keyHelp")}
          value={keyB64}
          autoComplete="new-password"
          onChange={(e) => setKeyB64(e.currentTarget.value)}
          data-testid="org-encryption-key-input"
        />
      )}

      <Alert color="yellow" variant="light">
        {t("orgEncryptionTab.rotationNote")}
      </Alert>

      <Group gap="sm" align="center">
        <Button
          onClick={apply}
          loading={saving}
          disabled={mode === "supply" && !keyB64.trim()}
          data-testid="org-encryption-apply"
        >
          {s.configured ? t("orgEncryptionTab.rotate") : t("orgEncryptionTab.set")}
        </Button>
        {msg && <Text c="green">{msg}</Text>}
        {error && <Text c="red">{error}</Text>}
      </Group>
    </Stack>
  );
}
