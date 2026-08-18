// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1486: the org_admin's branding editor.
//
// Five text fields and a logo. The server validates every one of them (core/org_branding.py) and
// answers 422 with the offending field, which is what lands in the error alert — the page does not
// keep its own copy of the rules.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Button,
  ColorInput,
  Group,
  Image,
  Stack,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import type { OrgBranding } from "../api/branding";
import {
  deleteOrgLogo,
  fetchOrgBranding,
  publicLogoUrl,
  saveOrgBranding,
  uploadOrgLogo,
} from "../api/branding";
import { applyOrgBranding } from "../lib/orgBranding";

export function OrgBrandingSettings({
  orgId,
  onError,
}: {
  orgId: string;
  onError: (e: unknown) => void;
}) {
  const { t } = useTranslation();
  const [branding, setBranding] = useState<OrgBranding>({});
  const [hasLogo, setHasLogo] = useState(false);
  // Bumped after every logo write so the preview <img> refetches instead of showing the old bytes.
  const [logoVersion, setLogoVersion] = useState(0);
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    fetchOrgBranding(orgId)
      .then((read) => {
        setBranding(read.branding);
        setHasLogo(read.logo_media_type !== null);
      })
      .catch(onError);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onError is re-created each render, so depending on it refetches in a loop; the read is keyed on the org alone
  }, [orgId]);

  const field = (key: keyof OrgBranding) => ({
    value: branding[key] ?? "",
    onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
      setBranding((prev) => ({ ...prev, [key]: e.currentTarget.value })),
  });

  const handleSave = async () => {
    try {
      const saved = await saveOrgBranding(orgId, branding);
      setBranding(saved);
      applyOrgBranding(saved);
      notifications.show({ color: "green", message: t("orgBranding.saved") });
    } catch (e) {
      onError(e);
    }
  };

  const handleUpload = async (file: File) => {
    try {
      await uploadOrgLogo(orgId, file);
      setHasLogo(true);
      setLogoVersion((v) => v + 1);
      notifications.show({ color: "green", message: t("orgBranding.logoSaved") });
    } catch (e) {
      onError(e);
    }
  };

  const handleRemoveLogo = async () => {
    try {
      await deleteOrgLogo(orgId);
      setHasLogo(false);
      setLogoVersion((v) => v + 1);
      notifications.show({ color: "green", message: t("orgBranding.logoRemoved") });
    } catch (e) {
      onError(e);
    }
  };

  return (
    <Stack gap="sm" maw={520} data-testid="org-branding-settings">
      <Title order={4}>{t("orgBranding.heading")}</Title>
      <Text c="dimmed" size="sm">
        {t("orgBranding.help")}
      </Text>

      <TextInput
        label={t("orgBranding.displayNameLabel")}
        description={t("orgBranding.displayNameDesc")}
        data-testid="branding-display-name"
        {...field("display_name")}
      />
      <ColorInput
        label={t("orgBranding.primaryColorLabel")}
        description={t("orgBranding.primaryColorDesc")}
        format="hex"
        value={branding.primary_color ?? ""}
        onChange={(value) => setBranding((prev) => ({ ...prev, primary_color: value }))}
        data-testid="branding-primary-color"
      />
      <ColorInput
        label={t("orgBranding.accentColorLabel")}
        description={t("orgBranding.accentColorDesc")}
        format="hex"
        value={branding.accent_color ?? ""}
        onChange={(value) => setBranding((prev) => ({ ...prev, accent_color: value }))}
        data-testid="branding-accent-color"
      />
      <Textarea
        label={t("orgBranding.welcomeLabel")}
        description={t("orgBranding.welcomeDesc")}
        autosize
        minRows={2}
        data-testid="branding-welcome"
        {...field("welcome_message")}
      />
      <Textarea
        label={t("orgBranding.inviteLabel")}
        description={t("orgBranding.inviteDesc")}
        autosize
        minRows={2}
        data-testid="branding-invite-message"
        {...field("invite_message")}
      />

      <Stack gap="xs">
        <Text size="sm" fw={500}>
          {t("orgBranding.logoLabel")}
        </Text>
        <Text c="dimmed" size="xs">
          {t("orgBranding.logoDesc")}
        </Text>
        {hasLogo ? (
          <Image
            src={`${publicLogoUrl(orgId)}&v=${logoVersion}`}
            alt={branding.display_name ?? orgId}
            h={48}
            w="auto"
            fit="contain"
            data-testid="branding-logo-preview"
          />
        ) : (
          <Text c="dimmed" size="sm" data-testid="branding-no-logo">
            {t("orgBranding.noLogo")}
          </Text>
        )}
        <input
          ref={fileRef}
          type="file"
          accept="image/png,image/jpeg,image/svg+xml,image/webp"
          style={{ display: "none" }}
          data-testid="branding-logo-input"
          onChange={(e) => {
            const file = e.currentTarget.files?.[0];
            e.currentTarget.value = "";
            if (file) void handleUpload(file);
          }}
        />
        <Group gap="xs">
          <Button
            variant="default"
            size="compact-sm"
            onClick={() => fileRef.current?.click()}
            data-testid="branding-logo-upload"
          >
            {hasLogo ? t("orgBranding.replaceLogo") : t("orgBranding.uploadLogo")}
          </Button>
          {hasLogo && (
            <Button
              variant="light"
              color="red"
              size="compact-sm"
              onClick={handleRemoveLogo}
              data-testid="branding-logo-remove"
            >
              {t("orgBranding.removeLogo")}
            </Button>
          )}
        </Group>
      </Stack>

      <Button onClick={handleSave} style={{ alignSelf: "flex-start" }} data-testid="branding-save">
        {t("orgBranding.save")}
      </Button>
    </Stack>
  );
}
