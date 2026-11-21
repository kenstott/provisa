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
  Loader,
  Modal,
  Progress,
  SimpleGrid,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useAuth } from "../../context/AuthContext";
import {
  BillingError,
  cancelTrial,
  checkoutReturnUrl,
  clearCheckoutReturn,
  fetchBillingSummary,
  fetchPortalUrl,
  readCheckoutReturn,
  startTrial,
  changePlan,
  fetchPlans,
  type BillingSummary,
  type CheckoutSession,
  type PlanChanged,
  type PlanOffer,
  type PlanOffers,
} from "../../api/billing";
import { useCheckoutAppearance } from "../../api/checkoutAppearance";
import { formatMoney, formatRate, planName } from "../../lib/planDisplay";
import { FormFooter } from "../form/FormLayout";
import { CheckoutModal } from "../CheckoutModal";

const GIB = 1024 ** 3;

function formatBytes(n: number): string {
  return `${(n / GIB).toFixed(2)} GB`;
}

/** A billing date: the merchant of record's periods start and renew on calendar days, sent as
 *  midnight UTC, so the day is read in UTC -- in a browser west of UTC the local date of that
 *  instant is the day before. */
function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
}

/** Whole days from now until `iso`, floored at zero. */
function daysUntil(iso: string): number {
  return Math.max(0, Math.ceil((new Date(iso).getTime() - Date.now()) / 86_400_000));
}

/**
 * REQ-1511: the four orderable plans, and the move between them.
 *
 * Every figure on a card is the server's: the price comes from the merchant of record's own price
 * object and the machine from the size table that provisions the engine (REQ-1449), because a rate
 * or a vCPU count written into the page drifts from the invoice and from the pod the organization
 * is actually given. What the page states for itself is only what the change will do — no
 * proration: each plan's minimum is billed in full (REQ-1509) — and that a plan change moves the
 * engine (REQ-1510).
 */
function PlanCards({ orgId, onChanged }: { orgId: string; onChanged: () => void }) {
  const { t } = useTranslation();
  const [offers, setOffers] = useState<PlanOffers | null>(null);
  const [target, setTarget] = useState<PlanOffer | null>(null);
  const [changing, setChanging] = useState(false);
  const [result, setResult] = useState<PlanChanged | null>(null);
  const [refused, setRefused] = useState("");

  const loadPlans = useCallback(() => {
    fetchPlans(orgId)
      .then(setOffers)
      .catch((e: Error) => setRefused(e.message));
  }, [orgId]);

  useEffect(loadPlans, [loadPlans]);

  if (offers === null) return null;

  // A fixed price as it is said: the amount, its interval where it has one, and whether it is a
  // minimum that metered usage counts against.
  const price = (cents: number, interval: string | null, kind: string | null) => {
    const amount = formatMoney(cents);
    if (interval && kind === "minimum")
      return t("billingTab.pricePerMinimum", { amount, interval });
    if (interval) return t("billingTab.pricePer", { amount, interval });
    return kind === "minimum" ? t("billingTab.priceMinimum", { amount }) : amount;
  };

  const confirm = async () => {
    if (target === null) return;
    setChanging(true);
    setRefused("");
    setResult(null);
    try {
      const changed = await changePlan(orgId, target.plan);
      setResult(changed);
      setTarget(null);
      loadPlans();
      onChanged();
    } catch (e) {
      // The server's words verbatim: the source count that refuses a Starter downgrade is the
      // server's at the moment of the change, and restating it here would restate a stale one.
      setRefused(e instanceof BillingError ? e.message : String(e));
      setTarget(null);
    } finally {
      setChanging(false);
    }
  };

  return (
    <Card withBorder padding="lg" data-testid="billing-plans">
      <Title order={4} mb="sm">
        {t("billingTab.plansTitle")}
      </Title>
      <Stack gap="sm">
        {refused && (
          <Alert
            color="red"
            title={t("billingTab.planChangeTitle")}
            data-testid="billing-plan-refused"
          >
            {refused}
          </Alert>
        )}
        {result && (
          <Alert
            color={result.engine_error ? "orange" : "green"}
            title={t("billingTab.nowOn", { plan: planName(result.plan) })}
            data-testid="billing-plan-changed"
          >
            <Stack gap="xs">
              <Text size="sm" data-testid="billing-plan-minimums">
                {t("billingTab.minimumsCharged")}
              </Text>
              {result.engine_error ? (
                <Text size="sm">
                  {t("billingTab.engineMoveFailed", { error: result.engine_error })}
                </Text>
              ) : result.engine?.lane === "isolated" ? (
                <Text size="sm">
                  {result.engine.size
                    ? t("billingTab.engineDedicatedSized", {
                        machine: result.engine.size.machine_type,
                        size: result.engine.size.label,
                      })
                    : t("billingTab.engineDedicated", { machine: "" })}
                </Text>
              ) : result.engine?.lane === "shared" ? (
                <Text size="sm">{t("billingTab.engineShared")}</Text>
              ) : null}
            </Stack>
          </Alert>
        )}
        <SimpleGrid cols={{ base: 1, sm: 2, lg: 4 }}>
          {offers.plans.map((offer) => {
            const current = offer.plan === offers.plan;
            return (
              <Card
                withBorder
                padding="md"
                key={offer.plan}
                data-testid={`billing-plan-${offer.plan}`}
              >
                <Stack gap="xs" h="100%" justify="space-between">
                  <Stack gap="xs">
                    <Group justify="space-between">
                      <Text fw={600}>{planName(offer.plan)}</Text>
                      {current && (
                        <Badge color="green" data-testid={`billing-plan-current-${offer.plan}`}>
                          {t("billingTab.currentPlan")}
                        </Badge>
                      )}
                    </Group>
                    {offer.fixed_cents != null && (
                      <Text size="sm">
                        {price(offer.fixed_cents, offer.fixed_interval, offer.fixed_kind)}
                      </Text>
                    )}
                    <Text size="sm" c="dimmed">
                      {offer.engine
                        ? t("billingTab.offerDedicated", {
                            machine: offer.engine.machine_type,
                            vcpu: offer.engine.vcpu,
                            memory: offer.engine.memory_gib,
                          })
                        : t("billingTab.offerShared")}
                    </Text>
                    <Text size="sm" c="dimmed">
                      {t("billingTab.sourceLimit", { limit: offer.source_limit })}
                    </Text>
                  </Stack>
                  {!current && (
                    <Button
                      variant="light"
                      style={{ alignSelf: "flex-start" }}
                      onClick={() => setTarget(offer)}
                      data-testid={`billing-choose-${offer.plan}`}
                    >
                      {t("billingTab.changeTo", { plan: planName(offer.plan) })}
                    </Button>
                  )}
                </Stack>
              </Card>
            );
          })}
        </SimpleGrid>
      </Stack>

      <Modal
        opened={target !== null}
        onClose={() => (changing ? undefined : setTarget(null))}
        title={target ? t("billingTab.changeTo", { plan: planName(target.plan) }) : ""}
      >
        {target && (
          <Stack gap="sm">
            <Text size="sm" data-testid="billing-plan-confirm-minimums">
              {t("billingTab.confirmMinimums")}
            </Text>
            <Text size="sm">
              {target.engine
                ? t("billingTab.confirmDedicated", { machine: target.engine.machine_type })
                : t("billingTab.confirmShared")}
            </Text>
            {offers.on_trial && <Text size="sm">{t("billingTab.trialEndsOnChange")}</Text>}
            {changing && (
              <Group gap="xs" data-testid="billing-plan-progress">
                <Loader size="xs" />
                <Text size="sm">
                  {target.engine
                    ? t("billingTab.progressStarting")
                    : t("billingTab.progressMoving")}
                </Text>
              </Group>
            )}
            <FormFooter
              onCancel={() => setTarget(null)}
              cancelProps={{ disabled: changing }}
              action={
                <Button onClick={confirm} loading={changing} data-testid="billing-plan-confirm">
                  {t("billingTab.changePlan")}
                </Button>
              }
            />
          </Stack>
        )}
      </Modal>
    </Card>
  );
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
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const appearance = useCheckoutAppearance();
  const [summary, setSummary] = useState<BillingSummary | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [checkout, setCheckout] = useState<CheckoutSession | null>(null);
  // REQ-1469: the rates the trial card quotes are the server's, from the merchant of
  // record's own prices. When they cannot be read the cards say so; they never quote a rate
  // written into this page.
  const [starter, setStarter] = useState<PlanOffer | null>(null);
  const [pricesError, setPricesError] = useState("");

  const load = useCallback(() => {
    if (!activeOrgId) return;
    fetchBillingSummary(activeOrgId)
      .then(setSummary)
      .catch((e: Error) => setError(e.message));
    fetchPlans(activeOrgId)
      .then((offers) => {
        const offer = offers.plans.find((p) => p.plan === "starter");
        if (offer === undefined) {
          setPricesError(t("billingTab.noStarterOffer"));
          return;
        }
        setStarter(offer);
        setPricesError("");
      })
      .catch((e: Error) => setPricesError(e.message));
  }, [activeOrgId, t]);

  useEffect(load, [load]);

  // REQ-1476: a completed checkout navigates back here with its session id. The summary load above
  // is the refetch; the parameter is only taken off the address so a reload does not carry it.
  useEffect(() => {
    if (readCheckoutReturn() !== null) clearCheckoutReturn();
  }, []);

  if (!activeOrgId) {
    return <Text c="dimmed">{t("billingTab.selectOrg")}</Text>;
  }

  const begin = async () => {
    setBusy(true);
    setError("");
    try {
      // The checkout is embedded in this page. Payment still happens entirely at the merchant of
      // record, and the subscription is linked to the org by the webhook (REQ-1469); on completion
      // Stripe returns to this page, which loads the summary afresh.
      setCheckout(await startTrial(activeOrgId, checkoutReturnUrl(), appearance));
    } catch (e) {
      setError(e instanceof BillingError ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const openPortal = async () => {
    setBusy(true);
    setError("");
    try {
      // The portal opens in place of this page; the button is free again either way.
      window.location.href = await fetchPortalUrl(activeOrgId, window.location.href);
    } catch (e) {
      setError((e as Error).message);
    } finally {
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
        <Title order={3}>{t("billingTab.title")}</Title>
        <Text c="dimmed" size="sm">
          {t("billingTab.intro")}
        </Text>
      </div>

      {error && (
        <Alert color="red" title={t("billingTab.title")} data-testid="billing-error">
          {error}
        </Alert>
      )}

      {offerTrial && (
        <Card withBorder padding="lg">
          <Stack gap="sm">
            <Title order={4}>{t("billingTab.trialOfferTitle")}</Title>
            <Text size="sm">{t("billingTab.trialOfferIntro")}</Text>
            <List size="sm">
              <List.Item>{t("billingTab.trialLimitDays")}</List.Item>
              <List.Item>{t("billingTab.trialLimitHours")}</List.Item>
              <List.Item>{t("billingTab.trialLimitEgress")}</List.Item>
            </List>
            <Text size="sm" data-testid="billing-trial-terms">
              {t("billingTab.trialTerms")}
              {starter !== null && starter.fixed_cents !== null && starter.fixed_interval && (
                <>
                  {" "}
                  {t("billingTab.trialRates", {
                    hourly: formatRate(starter.hourly_cents),
                    minimum: t("billingTab.pricePer", {
                      amount: formatMoney(starter.fixed_cents),
                      interval: starter.fixed_interval,
                    }),
                    includedGb: starter.egress.included_gb,
                    perGb: formatRate(starter.egress.per_gb_cents),
                  })}
                </>
              )}
            </Text>
            {pricesError && (
              <Alert color="yellow" data-testid="billing-prices-unavailable">
                {t("billingTab.pricesUnavailable", { reason: pricesError })}
              </Alert>
            )}
            <Group>
              <Button onClick={begin} loading={busy} data-testid="billing-start-trial">
                {t("billingTab.startTrial")}
              </Button>
            </Group>
          </Stack>
        </Card>
      )}

      {trial && (
        <Card withBorder padding="lg" data-testid="billing-trial">
          <Group justify="space-between" mb="sm">
            <Title order={4}>{t("billingTab.trialTitle")}</Title>
            {trialRunning ? (
              <Badge color="blue">
                {t("billingTab.daysLeft", { days: daysUntil(trial.ends_at) })}
              </Badge>
            ) : (
              <Badge color="green">
                {t("billingTab.converted", { date: formatDate(trial.converted_at!) })}
              </Badge>
            )}
          </Group>
          <Stack gap="xs">
            <Text size="sm">
              {t("billingTab.trialDates", {
                start: formatDate(trial.started_at),
                end: formatDate(trial.ends_at),
              })}
            </Text>
            <Text size="sm">
              {t("billingTab.activeHours", {
                used: trial.active_hours_used,
                included: trial.active_hours_included,
              })}
            </Text>
            <Progress
              value={(trial.active_hours_used / trial.active_hours_included) * 100}
              aria-label={t("billingTab.activeHoursAria")}
            />
            <Text size="sm">
              {t("billingTab.egress", {
                used: formatBytes(trial.egress_bytes_used),
                included: formatBytes(trial.egress_bytes_included),
              })}
            </Text>
            <Progress
              value={(trial.egress_bytes_used / trial.egress_bytes_included) * 100}
              aria-label={t("billingTab.egressAria")}
            />
            {trialRunning && !sub?.cancel_at_period_end && (
              <Group mt="sm">
                <Button
                  variant="default"
                  onClick={stopTrial}
                  loading={busy}
                  data-testid="billing-cancel-trial"
                >
                  {t("billingTab.cancelTrial")}
                </Button>
              </Group>
            )}
          </Stack>
        </Card>
      )}

      {summary && (
        <Card withBorder padding="lg">
          <Group justify="space-between" mb="sm">
            <Title order={4}>{t("billingTab.monthTitle")}</Title>
            <Badge color={summary.entitled ? "green" : "gray"}>
              {summary.subscription_status ?? summary.plan}
            </Badge>
          </Group>
          <Stack gap="xs">
            <Text size="sm">
              {t("billingTab.period", {
                start: formatDate(summary.period_start),
                end: formatDate(summary.period_end),
              })}
            </Text>
            {sub?.fixed_cents != null && (
              <Text size="sm">
                {t(sub.fixed_kind === "minimum" ? "billingTab.planMinimum" : "billingTab.planFee", {
                  // The label says whether it is a minimum; the price is the amount and interval.
                  price: sub.fixed_interval
                    ? t("billingTab.pricePer", {
                        amount: formatMoney(sub.fixed_cents),
                        interval: sub.fixed_interval,
                      })
                    : formatMoney(sub.fixed_cents),
                })}
              </Text>
            )}
            {/* Quantities, not money: the metered charge is the one the merchant of record
                invoices from posted usage records, and a total computed here would be a second
                number that disagrees with it. */}
            <Text size="sm">
              {t("billingTab.activeHoursMetered", { hours: summary.active_hours })}
            </Text>
            <Text size="sm">
              {t("billingTab.egressMetered", { amount: formatBytes(summary.egress_bytes) })}
            </Text>
          </Stack>
        </Card>
      )}

      {summary?.bill && (
        <Card withBorder padding="lg" data-testid="billing-current-bill">
          <Title order={4} mb="sm">
            {t("billingTab.billTitle")}
          </Title>
          <Stack gap="xs">
            <Text size="sm">
              {t("billingTab.period", {
                start: formatDate(summary.bill.period_start),
                end: formatDate(summary.bill.period_end),
              })}
            </Text>
            <Text size="sm">
              {t(
                summary.bill.fixed_kind === "minimum"
                  ? "billingTab.planMinimum"
                  : "billingTab.planFee",
                { price: formatMoney(summary.bill.fixed_cents) },
              )}
            </Text>
            {/* REQ-1469: each amount is the server's, priced by the merchant of record's own
                price from the usage already posted to it. Nothing is multiplied out here. */}
            {summary.bill.lines.map((line) => (
              <Text size="sm" key={line.kind} data-testid={`billing-line-${line.kind}`}>
                {t(
                  line.kind === "active_hours"
                    ? "billingTab.billLineHours"
                    : "billingTab.billLineTransfer",
                  {
                    posted: line.posted,
                    included: line.included,
                    rate: formatRate(line.rate_cents),
                    amount: formatMoney(line.cents),
                  },
                )}
              </Text>
            ))}
            <Text size="sm" fw={600} data-testid="billing-bill-total">
              {t("billingTab.billTotal", { amount: formatMoney(summary.bill.total_cents) })}
            </Text>
          </Stack>
        </Card>
      )}

      {summary?.bill_unavailable && (
        <Alert
          color="yellow"
          title={t("billingTab.billTitle")}
          data-testid="billing-bill-unavailable"
        >
          {t("billingTab.billUnavailable", { reason: summary.bill_unavailable })}
        </Alert>
      )}

      {sub !== null && <PlanCards orgId={activeOrgId} onChanged={load} />}

      {sub && (
        <Card withBorder padding="lg" data-testid="billing-next-charge">
          <Title order={4} mb="sm">
            {t("billingTab.nextChargeTitle")}
          </Title>
          <Stack gap="xs">
            {sub.status === "canceled" || sub.status === "incomplete_expired" ? (
              <Text size="sm">
                {t(
                  `billingTab.${sub.status === "canceled" ? "subCancelled" : "subExpired"}${
                    sub.ends_at ? "Access" : ""
                  }`,
                  { date: sub.ends_at ? formatDate(sub.ends_at) : "" },
                )}
              </Text>
            ) : sub.status === "past_due" || sub.status === "unpaid" ? (
              <Text size="sm">{t("billingTab.paymentFailed")}</Text>
            ) : summary?.next_charge ? (
              <Text size="sm" data-testid="billing-next-charge-amount">
                {t("billingTab.nextChargeAmount", {
                  date: formatDate(summary.next_charge.date),
                  amount: formatMoney(summary.next_charge.cents),
                })}
              </Text>
            ) : sub.renews_at && summary?.bill_unavailable ? (
              <Text size="sm">
                {t("billingTab.nextChargeUnstated", {
                  date: formatDate(sub.renews_at),
                  reason: summary.bill_unavailable,
                })}
              </Text>
            ) : sub.renews_at ? (
              <Text size="sm">
                {sub.fixed_cents == null
                  ? formatDate(sub.renews_at)
                  : t(
                      sub.fixed_kind === "minimum"
                        ? "billingTab.renewMinimum"
                        : "billingTab.renewPlus",
                      { date: formatDate(sub.renews_at), amount: formatMoney(sub.fixed_cents) },
                    )}
              </Text>
            ) : (
              <Text size="sm">{t("billingTab.noRenewal")}</Text>
            )}
            {sub.cancel_at_period_end && sub.ends_at && (
              <Text size="sm" data-testid="billing-ends-at-period-end">
                {t("billingTab.endsAtPeriodEnd", { date: formatDate(sub.ends_at) })}
              </Text>
            )}
            {sub.card_brand && sub.card_last_four && (
              <Text size="sm" c="dimmed">
                {t("billingTab.cardEnding", { brand: sub.card_brand, last4: sub.card_last_four })}
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
                  {t("billingTab.portal")}
                </Button>
              </Group>
            )}
          </Stack>
        </Card>
      )}

      <CheckoutModal session={checkout} onClose={() => setCheckout(null)} />
    </Stack>
  );
}
