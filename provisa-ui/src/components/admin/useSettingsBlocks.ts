// Copyright (c) 2026 Kenneth Stott
// Canary: e80bc84c-3068-46af-a02f-e5b6b08bc156
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1349: the settings-block hook the cards share. It lives apart from the cards themselves
 * because a module that exports both components and a hook loses fast refresh.
 */

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { fetchSettings, updateSettings, type PlatformSettings } from "../../api/admin";

type Block = keyof PlatformSettings;

/**
 * Load settings once and save back only `blocks`.
 *
 * The PUT is gated per block server-side, so the payload is narrowed to what this card owns: a
 * card that sends the whole snapshot would drag every deployment-wide block into an org
 * administrator's save and be refused as a whole.
 */
export function useSettingsBlocks(blocks: Block[]) {
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const { t } = useTranslation();
  // The block list is a literal at every call site, so it is a fresh array each render; the joined
  // form is what `save` depends on, which keeps the callback stable without asking every caller to
  // memoize an array it never changes.
  const blockKey = blocks.join(",");

  useEffect(() => {
    fetchSettings().then(setSettings);
  }, []);

  const save = useCallback(async () => {
    if (!settings) return;
    setSaving(true);
    setMsg("");
    const payload: Record<string, unknown> = {};
    for (const block of blockKey.split(",") as Block[]) {
      const value = settings[block];
      if (value === undefined) continue;
      // The domain MODE is applied by the destructive /admin/domain-policy endpoint, never by a
      // normal save, so it is stripped from the naming block on its way out.
      if (block === "naming") {
        const { use_domains: _ud, default_domain: _dd, ...rest } = settings.naming;
        payload.naming = rest;
        continue;
      }
      payload[block] = value;
    }
    try {
      const result = await updateSettings(payload as Partial<PlatformSettings>);
      const base = result.updated.length
        ? t("adminPage.settingsUpdated", { fields: result.updated.join(", ") })
        : t("adminPage.settingsNoChanges");
      setMsg(result.restart_required ? t("adminPage.settingsRestartRequired", { base }) : base);
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [settings, t, blockKey]);

  return { settings, setSettings, save, saving, msg };
}
