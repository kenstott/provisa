// Copyright (c) 2026 Kenneth Stott
// Canary: e8e3eb12-11ca-4451-ad9c-2f5d7407e4db
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Alert, Group, Loader, Text } from "@mantine/core";
import { ModalLayout } from "./modal/ModalLayout";
import { useTranslation } from "react-i18next";
import { mountCheckout, type CheckoutSession, type EmbeddedCheckout } from "../api/billing";

/**
 * Stripe's embedded checkout, hosted in the app's standard modal (REQ-1476).
 *
 * The checkout is mounted while `session` is set and destroyed when it is cleared or the modal is
 * closed, so closing without paying leaves the page as it was. Completing payment is not reported
 * here: Stripe navigates the page to the checkout's return URL, and the page picks it up there.
 */
export function CheckoutModal({
  session,
  onClose,
}: {
  session: CheckoutSession | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  // A state ref, not a ref object: the modal renders its body in a portal after it opens, and the
  // mount has to run once that element exists.
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  // Both are recorded against the session they belong to, so a new session starts unmounted and
  // without the last one's failure.
  const [mountedFor, setMountedFor] = useState<CheckoutSession | null>(null);
  const [failure, setFailure] = useState<{ session: CheckoutSession; message: string } | null>(
    null,
  );
  const mounted = session !== null && mountedFor === session;
  const error = session !== null && failure?.session === session ? failure.message : "";

  useEffect(() => {
    if (session === null || container === null) return;
    let cancelled = false;
    let checkout: EmbeddedCheckout | null = null;
    mountCheckout(session, container)
      .then((c) => {
        if (cancelled) {
          c.destroy();
          return;
        }
        checkout = c;
        setMountedFor(session);
      })
      .catch((e: Error) => {
        if (!cancelled) setFailure({ session, message: e.message });
      });
    return () => {
      cancelled = true;
      checkout?.destroy();
    };
  }, [session, container]);

  return (
    <ModalLayout
      opened={session !== null}
      onClose={onClose}
      title={t("billingTab.checkoutTitle")}
      size="xl"
      centered
    >
      {error && (
        <Alert color="red" mb="sm" data-testid="checkout-error">
          {t("billingTab.checkoutFailed", { error })}
        </Alert>
      )}
      {!mounted && !error && (
        <Group gap="xs" data-testid="checkout-loading">
          <Loader size="xs" />
          <Text size="sm">{t("billingTab.checkoutLoading")}</Text>
        </Group>
      )}
      <div ref={setContainer} data-testid="checkout-container" />
    </ModalLayout>
  );
}
