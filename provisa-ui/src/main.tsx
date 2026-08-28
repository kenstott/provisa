// Copyright (c) 2026 Kenneth Stott
// Canary: 00916f3a-8398-45ce-9f67-c6a98a0c948d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createRoot } from "react-dom/client";
// Install the same-origin bearer-token fetch interceptor before anything issues a request.
import { installAuthFetch } from "./lib/authFetch.ts";
// Mantine styles must load before app CSS so local overrides win the cascade.
import "@mantine/core/styles.css";
import "@mantine/dates/styles.css";
import "@mantine/notifications/styles.css";
import { DirectionProvider, MantineProvider, localStorageColorSchemeManager } from "@mantine/core";
import { Notifications } from "@mantine/notifications";
import { I18nextProvider } from "react-i18next";
import { theme } from "./theme/theme.ts";
import "./theme/tokens.css";
import i18n from "./i18n/index.ts";
import { DirectionSync } from "./i18n/direction.tsx";
import "./index.css";
import App from "./App.tsx";
import { isOrgSubdomainHost } from "./lib/authHost.ts";
import { LOGOUT_PATH, signOut } from "./lib/session.ts";

declare global {
  interface Window {
    __provisaHideSplash?: () => void;
  }
}

// Key must match the inline anti-flash script in index.html.
installAuthFetch();

const colorSchemeManager = localStorageColorSchemeManager({
  key: "provisa-color-scheme",
});

// REQ-1266: block the first render until the Firebase bearer is settled so the dashboard's
// initial queries never fire against a stale/expired localStorage token (the boot-time race
// that 401'd every call on reload of an hour-old session). installFirebaseTokenSync resolves
// as soon as the first token state settles — a fresh token written, or cleared when signed
// out / refresh rejected — and no-ops (resolves immediately) on non-firebase deploys. The
// Firebase SDK is dynamically imported so it stays in its own chunk, loaded only here.
async function bootstrap() {
  // REQ-1603: /logout is an entry point, not a route — it is where an org subdomain sends the
  // browser to end a session it does not hold. Answer it before React mounts: the app rendered
  // under a still-valid token would fire its signed-in queries during the sign-out it is here to
  // perform. signOut ends with a navigation, so nothing after this runs.
  if (window.location.pathname === LOGOUT_PATH) {
    await signOut();
    return;
  }

  // REQ-1348: an org subdomain has no session of its own — sign-in runs only on the control
  // plane (lib/authHost.ts) — so it borrows the bearer from there instead of starting Firebase
  // locally, where signInWithPopup would fail with auth/unauthorized-domain. Returns false while
  // redirecting to the control-plane login, in which case there is nothing worth rendering.
  if (isOrgSubdomainHost()) {
    const { establishOrgSubdomainSession } = await import("./lib/crossSubdomainAuth.ts");
    if (!(await establishOrgSubdomainSession())) return;
    // REQ-1486: on an org's own address the Host names the org, so its branding is read without a
    // header and applied before the first paint. A branding read that fails is not a boot failure —
    // the product's own presentation is the correct fallback (REQ-1486 says branding is additive).
    const [{ fetchPublicBranding }, { applyOrgBranding }] = await Promise.all([
      import("./api/branding.ts"),
      import("./lib/orgBranding.ts"),
    ]);
    await fetchPublicBranding()
      .then((read) => applyOrgBranding(read.branding))
      .catch((err: unknown) => console.error("org branding could not be read:", err));
  } else {
    const firebase = await import("./lib/firebase.ts");
    await firebase.installFirebaseTokenSync();
  }

  createRoot(document.getElementById("root")!).render(
    <DirectionProvider>
      <MantineProvider
        theme={theme}
        defaultColorScheme="dark"
        colorSchemeManager={colorSchemeManager}
      >
        <I18nextProvider i18n={i18n}>
          <DirectionSync />
          <Notifications />
          <App />
        </I18nextProvider>
      </MantineProvider>
    </DirectionProvider>,
  );

  // Dismiss the pre-React convergence-splash once the app has mounted and painted its
  // first frame. The splash enforces its own minimum display time + fade, so this only
  // signals readiness.
  requestAnimationFrame(() => requestAnimationFrame(() => window.__provisaHideSplash?.()));
}

// A boot that throws leaves the pre-React splash covering a page that will never mount, which
// reads as a hang. Replace it with the failure so the cause is visible instead of hidden —
// notably REQ-1348's unreachable auth relay, which is a broken control plane, not a sign-out.
bootstrap().catch((err: unknown) => {
  console.error("Provisa failed to start:", err);
  window.__provisaHideSplash?.();
  const root = document.getElementById("root");
  if (root) {
    root.textContent = `Provisa failed to start: ${err instanceof Error ? err.message : String(err)}`;
    root.setAttribute("style", "padding:2rem;font-family:system-ui;color:#e5484d");
  }
});
