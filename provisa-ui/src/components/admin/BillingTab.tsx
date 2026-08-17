// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8a4b61-30cd-4e19-b7a5-9c0e3d16f8b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Card,
  Group,
  List,
  Progress,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { useAuth } from "../../context/AuthContext";
import {
  BillingError,
  cancelTrial,
  fetchBillingSummary,
  fetchPortalUrl,
  openCheckout,
  startEgressSubscription,
  startTrial,
  type BillingSummary,
} from "../../api/billing";

const GIB = 1024 ** 3;

function formatBytes(n: number): string {
  return `${(n / GIB).toFixed(2)} GB`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatMoney(cents: number): string {
  return `$${(cents / 100).toFixed(2)}`;
}

/** Whole days from now until `iso`, floored at zero. */
function daysUntil(iso: string): number {
  return Math.max(0, Math.ceil((new Date(iso).getTime() - Date.now()) / 86_400_000));
}

/**
 * REQ-1469: what this org is on, what it has used this month, and when the card is charged next.
 * REQ-1455: an org with neither subscription nor trial is offered the trial here — this is the only
 * place in the product that offers it, so its absence is the difference between a prospect who can
 * start an evaluation and one who cannot.
 *
 * Every amount on this page comes from the merchant of record or from Provisa's own meter, never
 * from a rate multiplied out in the browser: a bill the page computed a third way would disagree
 * with the invoice the customer receives.
 */
export function BillingTab() {
  const { activeOrgId } = useAuth();
  const [summary, setSummary] = useState<BillingSummary | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    if (!activeOrgId) return;
    fetchBillingSummary(activeOrgId)
      .then(setSummary)
      .catch((e: Error) => setError(e.message));
  }, [activeOrgId]);

  useEffect(load, [load]);

  if (!activeOrgId) {
    return <Text c="dimmed">Select an organization to see its billing.</Text>;
  }

  const begin = async () => {
    setBusy(true);
    setError("");
    try {
      // The checkout opens as an overlay on this page. Payment still happens entirely at the
      // merchant of record, and the subscription is linked to the org by the webhook, so a closed
      // overlay after payment still lands the plan (REQ-1469).
      await openCheckout(await startTrial(activeOrgId, window.location.href), load);
      setBusy(false);
    } catch (e) {
      setError(e instanceof BillingError ? e.message : String(e));
      setBusy(false);
    }
  };

  // REQ-1482: data transfer is a second subscription — a Lemon Squeezy variant carries one
  // usage-based price and the plan's is the active hour — so it is a second checkout.
  const addTransfer = async () => {
    setBusy(true);
    setError("");
    try {
      await openCheckout(await startEgressSubscription(activeOrgId, window.location.href), load);
      setBusy(false);
    } catch (e) {
      setError(e instanceof BillingError ? e.message : String(e));
      setBusy(false);
    }
  };

  const openPortal = async () => {
    setBusy(true);
    setError("");
    try {
      window.location.href = await fetchPortalUrl(activeOrgId);
    } catch (e) {
      setError((e as Error).message);
      setBusy(false);
    }
  };

  const stopTrial = async () => {
    setBusy(true);
    setError("");
    try {
      await cancelTrial(activeOrgId);
      load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const trial = summary?.trial ?? null;
  const trialRunning = trial !== null && trial.converted_at === null;
  const offerTrial = summary !== null && trial === null && summary.subscription === null;
  const sub = summary?.subscription ?? null;

  return (
    <Stack gap="md">
      <div>
        <Title order={3}>Billing</Title>
        <Text c="dimmed" size="sm">
          The plan this organization is on, what it has used so far this month, and when the card is
          charged next.
        </Text>
      </div>

      {error && (
        <Alert color="red" title="Billing" data-testid="billing-error">
          {error}
        </Alert>
      )}

      {offerTrial && (
        <Card withBorder padding="lg">
          <Stack gap="sm">
            <Title order={4}>Start your free Provisa Starter trial</Title>
            <Text size="sm">
              Evaluate Starter on live data for 14 days. The trial ends at whichever comes first:
            </Text>
            <List size="sm">
              <List.Item>14 days</List.Item>
              <List.Item>40 active hours — any hour in which you run at least one query</List.Item>
              <List.Item>25 GB of egress</List.Item>
            </List>
            <Text size="sm">
              A card is captured at signup as a $0 authorization and is not charged during the
              trial. Card details are entered at Lemon Squeezy, our merchant of record, and never
              reach Provisa. At the end of the trial the subscription converts to paid Starter —
              $1.30 per active hour against a $25 monthly minimum, 25 GB of egress included and
              $0.48 per GB beyond it. You can cancel at any point before it converts.
            </Text>
            <Group>
              <Button onClick={begin} loading={busy} data-testid="billing-start-trial">
                Start free trial
              </Button>
            </Group>
          </Stack>
        </Card>
      )}

      {trial && (
        <Card withBorder padding="lg" data-testid="billing-trial">
          <Group justify="space-between" mb="sm">
            <Title order={4}>Trial</Title>
            {trialRunning ? (
              <Badge color="blue">{daysUntil(trial.ends_at)} days left</Badge>
            ) : (
              <Badge color="green">Converted {formatDate(trial.converted_at!)}</Badge>
            )}
          </Group>
          <Stack gap="xs">
            <Text size="sm">
              Started {formatDate(trial.started_at)}, ends {formatDate(trial.ends_at)}.
            </Text>
            <Text size="sm">
              Active hours: {trial.active_hours_used} of {trial.active_hours_included}
            </Text>
            <Progress
              value={(trial.active_hours_used / trial.active_hours_included) * 100}
              aria-label="Trial active hours used"
            />
            <Text size="sm">
              Egress: {formatBytes(trial.egress_bytes_used)} of{" "}
              {formatBytes(trial.egress_bytes_included)}
            </Text>
            <Progress
              value={(trial.egress_bytes_used / trial.egress_bytes_included) * 100}
              aria-label="Trial egress used"
            />
            {trialRunning && (
              <Group mt="sm">
                <Button
                  variant="default"
                  onClick={stopTrial}
                  loading={busy}
                  data-testid="billing-cancel-trial"
                >
                  Cancel trial
                </Button>
              </Group>
            )}
          </Stack>
        </Card>
      )}

      {summary && (
        <Card withBorder padding="lg">
          <Group justify="space-between" mb="sm">
            <Title order={4}>This month</Title>
            <Badge color={summary.entitled ? "green" : "gray"}>
              {summary.subscription_status ?? summary.plan}
            </Badge>
          </Group>
          <Stack gap="xs">
            <Text size="sm">
              {formatDate(summary.period_start)} to {formatDate(summary.period_end)}
            </Text>
            {sub?.fixed_cents != null && (
              <Text size="sm">
                {sub.fixed_kind === "minimum" ? "Plan minimum" : "Plan fee"}:{" "}
                {formatMoney(sub.fixed_cents)}
                {sub.fixed_interval ? ` per ${sub.fixed_interval}` : ""}
                {sub.variant_name ? ` — ${sub.variant_name}` : ""}
              </Text>
            )}
            {/* Quantities, not money: the metered charge is the one the merchant of record
                invoices from posted usage records, and a total computed here would be a second
                number that disagrees with it. */}
            <Text size="sm">Active hours metered: {summary.active_hours}</Text>
            <Text size="sm">Egress metered: {formatBytes(summary.egress_bytes)}</Text>
          </Stack>
        </Card>
      )}

      {summary && sub !== null && !summary.has_egress_subscription && (
        <Card withBorder padding="lg" data-testid="billing-add-transfer">
          <Stack gap="sm">
            <Title order={4}>Data transfer</Title>
            <Text size="sm">
              Data transfer is billed on its own subscription — 25 GB included each month and $0.48
              per GB beyond it. This organization does not have one yet, so the transfer it is
              metering is not being billed.
            </Text>
            <Group>
              <Button
                onClick={addTransfer}
                loading={busy}
                data-testid="billing-add-transfer-button"
              >
                Add data transfer
              </Button>
            </Group>
          </Stack>
        </Card>
      )}

      {sub && (
        <Card withBorder padding="lg" data-testid="billing-next-charge">
          <Title order={4} mb="sm">
            Next charge
          </Title>
          <Stack gap="xs">
            {sub.status === "cancelled" || sub.status === "expired" ? (
              <Text size="sm">
                This subscription is {sub.status}
                {sub.ends_at ? ` and access ends ${formatDate(sub.ends_at)}` : ""}. There is no
                further charge.
              </Text>
            ) : sub.status === "past_due" ? (
              <Text size="sm">
                The last payment failed. Update the payment method in the customer portal to keep
                this organization running.
              </Text>
            ) : sub.renews_at ? (
              <Text size="sm">
                {formatDate(sub.renews_at)}
                {sub.fixed_cents == null
                  ? ""
                  : sub.fixed_kind === "minimum"
                    ? `, at least ${formatMoney(sub.fixed_cents)} — metered usage counts against that minimum`
                    : `, ${formatMoney(sub.fixed_cents)} plus metered usage`}
              </Text>
            ) : (
              <Text size="sm">No renewal date is set on this subscription.</Text>
            )}
            {sub.card_brand && sub.card_last_four && (
              <Text size="sm" c="dimmed">
                {sub.card_brand} ending {sub.card_last_four}
              </Text>
            )}
            {summary?.has_portal && (
              <Group mt="sm">
                <Button
                  variant="default"
                  onClick={openPortal}
                  loading={busy}
                  data-testid="billing-portal"
                >
                  Payment method and invoices
                </Button>
              </Group>
            )}
          </Stack>
        </Card>
      )}
    </Stack>
  );
}
