// Copyright (c) 2026 Kenneth Stott
// Canary: b4e99c24-2095-41bd-a8ef-06e400ad82fc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Anchor, Badge, Button, Group, Loader } from "@mantine/core";
import { ExternalLink, Wifi, WifiOff } from "lucide-react";
import "./DocsPage.css";

// The full MkDocs Material site is served two ways and the reader is online-first:
//  - online:  the hosted, always-current site (canonical URL below);
//  - offline: the same site, built into the app and served same-origin under
//    /docs-site/ so search, nav, and theming all work airgapped.
// A reachability probe picks the source; the user can override either way.
const ONLINE_URL = "https://provisa.dev/docs/";
const OFFLINE_URL = "/docs-site/";
const PROBE_TIMEOUT_MS = 2500;

type Source = "probing" | "online" | "offline";

export function DocsPage() {
  const { t } = useTranslation();
  const [probed, setProbed] = useState<Source>("probing");
  const [override, setOverride] = useState<"online" | "offline" | null>(null);

  // Probe the hosted docs once. navigator.onLine short-circuits the airgap case;
  // otherwise a no-cors HEAD with a timeout decides. A no-cors probe can't read
  // status, so it only proves the network round-trip succeeded — good enough to
  // prefer online, and the manual toggle covers a stale/misrouted host.
  useEffect(() => {
    let cancelled = false;
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      setProbed("offline");
      return;
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
    fetch(ONLINE_URL, { method: "HEAD", mode: "no-cors", signal: controller.signal })
      .then(() => {
        if (!cancelled) setProbed("online");
      })
      .catch(() => {
        if (!cancelled) setProbed("offline");
      })
      .finally(() => clearTimeout(timer));
    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, []);

  const source: Source = override ?? probed;
  const src = source === "online" ? ONLINE_URL : OFFLINE_URL;
  const online = source === "online";

  const toolbar = useMemo(
    () => (
      <Group className="docs-toolbar" justify="space-between" gap="xs">
        <Badge
          variant="light"
          color={online ? "teal" : "gray"}
          leftSection={online ? <Wifi size={12} /> : <WifiOff size={12} />}
          data-testid="docs-source"
        >
          {online ? t("docsPage.sourceOnline") : t("docsPage.sourceOffline")}
        </Badge>
        <Group gap="xs">
          <Button
            size="compact-xs"
            variant="subtle"
            data-testid="docs-toggle-source"
            onClick={() => setOverride(online ? "offline" : "online")}
          >
            {online ? t("docsPage.viewOffline") : t("docsPage.viewOnline")}
          </Button>
          <Anchor href={src} target="_blank" rel="noreferrer" size="xs">
            <Group gap={4} align="center">
              <ExternalLink size={12} />
              {t("docsPage.openInNewTab")}
            </Group>
          </Anchor>
        </Group>
      </Group>
    ),
    [online, src, t],
  );

  if (source === "probing") {
    return (
      <div className="docs-page docs-page--center" data-testid="docs-probing">
        <Loader size="sm" aria-label={t("docsPage.loading")} />
      </div>
    );
  }

  return (
    <div className="docs-page">
      {toolbar}
      <iframe
        className="docs-frame"
        src={src}
        title={t("docsPage.title")}
        data-testid="docs-frame"
        data-source={source}
      />
    </div>
  );
}
