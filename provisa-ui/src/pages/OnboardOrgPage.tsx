// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2a7d41-9c83-4e15-a0b6-2d7e91f4c8ab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Group,
  Loader,
  Paper,
  Radio,
  SegmentedControl,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { UserPlus } from "lucide-react";
import type { AutoJoinOffer, PendingInvite } from "../api/admin";
import {
  OrgError,
  acceptAutoJoin,
  createOrg,
  declineAutoJoin,
  fetchAutoJoinOffers,
  fetchMyInvites,
  fetchOrgStatus,
  redeemInvite,
} from "../api/admin";
import type { OrgReservation, PlanOffer } from "../api/billing";
import { useCheckoutAppearance } from "../api/checkoutAppearance";
import {
  BillingError,
  fetchCatalog,
  fetchMyReservation,
  openCheckout,
  reconcileCheckout,
  startEgressSubscription,
  startPlanCheckout,
} from "../api/billing";
import { useAuth } from "../context/AuthContext";
import { orgOrigin } from "../lib/authHost";
import { OrgWelcomePage } from "./OrgWelcomePage";
import { signOut } from "../lib/session";
import { formatMoney, planName } from "../lib/planDisplay";

// REQ-1266: a member-less authenticated user either self-creates an org OR joins an existing one
// with an invite code. Create returns immediately with provisioning_state="provisioning"; we poll
// /status until ready/failed. Join redeems the invite (server grants membership synchronously).
// Both then refetch identity (so the new membership clears the onboarding gate) and route in.
export function OnboardOrgPage() {
  const { t } = useTranslation();
  const { billing, email, selectOrg, refresh } = useAuth();
  const appearance = useCheckoutAppearance();
  const navigate = useNavigate();

  // REQ-1276: an org's home is its own host, so leaving onboarding for an org lands on that host
  // rather than staying on the control plane. It is a document load, not a client route, and that
  // is the point: the org then binds by Host instead of by the `X-Org-Provisa` header, and the
  // identity bootstrap re-runs against it, so the org_admin grant made moments earlier is in hand
  // before the first page renders. `orgOrigin` is null only where the host names no org (a desktop
  // install has no per-org address); there the header IS the binding and the in-app route is the
  // whole navigation.
  const enterOrg = (orgId: string, path: string) => {
    const address = orgOrigin(orgId);
    if (address) window.location.assign(`${address}${path}`);
    else navigate(path);
  };
  const [mode, setMode] = useState<"create" | "join">("create");
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [includeDemo, setIncludeDemo] = useState(false);
  // REQ-1043/REQ-1067: dedicated federation engine, chosen at creation. Only where nothing is sold —
  // on a commerce deploy the lane comes with the plan (REQ-1510), so it is not a separate choice.
  const [isolatedEngine, setIsolatedEngine] = useState(false);
  // REQ-1514: the priced plans, and the one being bought. The checkout overlay states only the
  // first tier's unit price, which is zero on every plan, so the terms are shown here instead.
  const [plans, setPlans] = useState<PlanOffer[] | null>(null);
  // REQ-1566: whether this account is still owed a free evaluation. Until the catalog answers,
  // no trial is claimed — promising free days and then billing on day one is the failure this
  // flag exists to prevent, and the optimistic default is the one that produces it.
  const [trialAvailable, setTrialAvailable] = useState(false);
  const [plan, setPlan] = useState<string | null>(null);
  const [emailRule, setEmailRule] = useState("");
  const [autoJoin, setAutoJoin] = useState(false);
  const [autoJoinRole, setAutoJoinRole] = useState("");
  // REQ-1567: the server measures how far the rule reaches and refuses a broad one until its
  // author accepts what it admits. The acceptance is offered only after that refusal, with the
  // addresses it named in hand — a checkbox shown before there is anything to accept would be
  // ticked out of habit, which is not consent to a risk nobody has been shown.
  const [breadthWarning, setBreadthWarning] = useState<string | null>(null);
  const [riskAcknowledged, setRiskAcknowledged] = useState(false);
  const [invite, setInvite] = useState("");
  const [error, setError] = useState<string | null>(null);
  // "creating" is the org create itself; "provisioning" is only reached after a checkout closed,
  // which is what lets the two say different things about payment on a commerce deploy.
  const [phase, setPhase] = useState<
    "form" | "creating" | "checkout" | "provisioning" | "joining" | "welcome"
  >("form");
  // REQ-1476: a reservation this account left behind — the id is held, so the way back in is the
  // checkout it abandoned, not a second create.
  const [reservation, setReservation] = useState<OrgReservation | null>(null);
  // REQ-1287: an invited user should be TOLD they were invited, not asked to produce a token they
  // may never have kept. Invites addressed to this identity's email are offered as one-click joins;
  // the token field stays for link invites, which are addressed to nobody.
  const [pendingInvites, setPendingInvites] = useState<PendingInvite[]>([]);
  // REQ-1568: orgs whose auto-join rule matches this address and that the server would not choose
  // between. A single match never reaches the page — it was joined at sign-in — so anything here
  // is a genuine ambiguity that only the person holding the address can settle.
  const [autoJoinOffers, setAutoJoinOffers] = useState<AutoJoinOffer[]>([]);

  useEffect(() => {
    let cancelled = false;
    fetchMyInvites()
      .then((invites) => {
        if (cancelled) return;
        setPendingInvites(invites);
      })
      .catch(() => {
        // A failed lookup must not block org creation — the token field still works. Nothing to
        // show, so leave the list empty rather than reporting an error the user cannot act on.
        if (!cancelled) setPendingInvites([]);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    fetchAutoJoinOffers()
      .then((offers) => {
        if (!cancelled) setAutoJoinOffers(offers);
      })
      .catch(() => {
        // Same reasoning as the invite lookup: a failed read must not stand between the user and
        // creating their own org. No offers shown is the safe reading — it joins nobody.
        if (!cancelled) setAutoJoinOffers([]);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // REQ-1476: only a commercial deployment reserves ids, so /billing/reservation exists only there.
  useEffect(() => {
    if (!billing) return;
    let cancelled = false;
    fetchMyReservation()
      .then((held) => {
        if (!cancelled) setReservation(held);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : t("onboardOrg.createFailed"));
      });
    return () => {
      cancelled = true;
    };
  }, [billing, t]);

  // REQ-1514: the catalog is priced by the store, so it is fetched rather than typed into the page.
  // Only a commercial deployment sells plans, so /billing/catalog exists only there.
  useEffect(() => {
    if (!billing) return;
    let cancelled = false;
    fetchCatalog()
      .then((catalog) => {
        if (cancelled) return;
        setPlans(catalog.plans);
        setTrialAvailable(catalog.trial_available);
        // The cheapest plan is first (PLAN_ORDER) and is the one carrying the trial.
        setPlan(catalog.plans[0].plan);
      })
      .catch((err) => {
        if (!cancelled)
          setError(err instanceof Error ? err.message : t("onboardOrg.planLoadFailed"));
      });
    return () => {
      cancelled = true;
    };
  }, [billing, t]);

  // The plan being bought, once the catalog has arrived. Null on a non-commercial deployment and
  // while the catalog is in flight, which is what the submit button is disabled on.
  const offer = plans === null ? null : (plans.find((p) => p.plan === plan) ?? null);

  const acceptInvite = async (token: string) => {
    setError(null);
    setPhase("joining");
    try {
      const { org_id } = await redeemInvite(token);
      selectOrg(org_id);
      await refresh();
      enterOrg(org_id, "/query");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.joinFailed"));
      setPhase("form");
    }
  };

  const joinOfferedOrg = async (orgId: string) => {
    setError(null);
    setPhase("joining");
    try {
      const { org_id } = await acceptAutoJoin(orgId);
      selectOrg(org_id);
      await refresh();
      enterOrg(org_id, "/query");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.joinFailed"));
      setPhase("form");
    }
  };

  // Declining is recorded server-side (REQ-1306) so the question is not put again at the next
  // sign-in; the page then looks exactly as it would for someone no rule matched.
  const declineOfferedOrgs = async () => {
    setError(null);
    try {
      await declineAutoJoin();
      setAutoJoinOffers([]);
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.joinFailed"));
    }
  };

  // Bounded poll — the background provisioning task flips the row.
  const waitForReady = async (orgId: string, from: string) => {
    let state = from;
    for (let i = 0; i < 300 && state === "provisioning"; i++) {
      await new Promise((r) => setTimeout(r, 1000));
      const status = await fetchOrgStatus(orgId);
      state = status.provisioning_state;
      if (state === "failed") {
        throw new Error(status.provisioning_error || t("onboardOrg.provisionFailed"));
      }
    }
    if (state !== "ready") throw new Error(t("onboardOrg.provisionTimeout"));
    // Bind the new org + refresh identity so the org_admin grant resolves BEFORE the welcome
    // screen offers links into /team (user_management) and /security/roles (access_config).
    selectOrg(orgId);
    await refresh();
  };

  // REQ-1476: the org leaves awaiting_checkout when the purchase binds to it — normally through the
  // webhook, and through reconcile when that has not arrived. Both read the same purchase, and the
  // store publishes it some seconds after the overlay closes, so a reconcile that finds no
  // subscription yet is the race and not the answer: keep asking until one of the two lands.
  const settleCheckout = async (orgId: string) => {
    let state = (await fetchOrgStatus(orgId)).provisioning_state;
    for (let i = 0; i < 60 && state === "awaiting_checkout"; i++) {
      await new Promise((r) => setTimeout(r, 2000));
      try {
        state = (await reconcileCheckout(orgId)).state;
      } catch (err) {
        if (!(err instanceof BillingError) || err.code !== "billing.no_subscription_found")
          throw err;
        state = (await fetchOrgStatus(orgId)).provisioning_state;
      }
    }
    if (state === "awaiting_checkout") throw new Error(t("onboardOrg.checkoutNotSettled"));
    return state;
  };

  // REQ-1476: the subscription is what builds the org, so the checkout overlay is the rest of the
  // create. The webhook normally provisions before the overlay closes; reconcile covers the case
  // where it has not arrived, and the poll then runs the same way for both.
  const runCheckout = async (orgId: string, chosen: string) => {
    setPhase("checkout");
    const url = await startPlanCheckout(orgId, chosen, window.location.href, appearance);
    await openCheckout(
      url,
      () => {
        void (async () => {
          setPhase("provisioning");
          try {
            await waitForReady(orgId, await settleCheckout(orgId));
          } catch (err) {
            setError(err instanceof Error ? err.message : t("onboardOrg.createFailed"));
            setPhase("form");
            return;
          }
          // REQ-1482: the transfer subscription is a second checkout — one Lemon Squeezy variant
          // carries one usage-based price, and the plan's is the active hour. Ordered after the org
          // is built, which is when its billing row exists to bind the subscription to. A failure
          // here is reported over the welcome screen: the org is provisioned and the create is done.
          try {
            const egress = await startEgressSubscription(orgId, window.location.href, appearance);
            await openCheckout(egress, () => {
              // The webhook binds the subscription; nothing on this page reads it.
            });
            // REQ-1276: the create is finished, so it lands on the org's own host — the welcome is
            // read at `{org}.provisa.dev`, which is the address the org answers at from here on.
            enterOrg(orgId, "/welcome");
          } catch (err) {
            // Stay on the control plane instead: the welcome is shown here, under what failed,
            // because a document load to the org host would take the report with it.
            setError(err instanceof Error ? err.message : t("onboardOrg.egressCheckoutFailed"));
            setPhase("welcome");
          }
        })();
      },
      () => {
        // REQ-1476: closing the overlay abandons the checkout, not the org — the id stays reserved
        // until it expires. Come back to the form holding that reservation, so the way on is the
        // resume it offers; re-entering the same id would be refused as taken.
        void (async () => {
          setPhase("form");
          try {
            setReservation(await fetchMyReservation());
          } catch (err) {
            setError(err instanceof Error ? err.message : t("onboardOrg.createFailed"));
          }
        })();
      },
    );
  };

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setPhase("creating");
    try {
      const created = await createOrg(
        id,
        name,
        includeDemo,
        {
          emailRule: emailRule.trim() || null,
          autoJoin,
          autoJoinRole: autoJoinRole.trim() || null,
          riskAcknowledged,
        },
        // REQ-1510: where plans are sold the lane is the plan's, not a checkbox's.
        offer ? offer.lane === "isolated" : isolatedEngine,
      );
      if (created.provisioning_state === "awaiting_checkout") {
        // The server only holds an org for checkout where plans are sold, and the submit that got
        // here is disabled until one is picked, so a missing offer is a broken page, not a case.
        if (offer === null) throw new Error(t("onboardOrg.planLoadFailed"));
        await runCheckout(id, offer.plan);
        return;
      }
      await waitForReady(id, created.provisioning_state);
      enterOrg(id, "/welcome");
      return;
    } catch (err) {
      if (err instanceof OrgError && err.code === "orgs.auto_join_breadth_unacknowledged") {
        // REQ-1567: not a failure to report and move on from — it is the question the rule raised,
        // asked with the addresses it would admit, and the form waits on the answer.
        setBreadthWarning(err.message);
        setError(null);
      } else {
        setError(err instanceof Error ? err.message : t("onboardOrg.createFailed"));
      }
      setPhase("form");
    }
  };

  const handleResume = async (held: OrgReservation, chosen: PlanOffer) => {
    setError(null);
    setId(held.org_id);
    setName(held.name);
    try {
      await runCheckout(held.org_id, chosen.plan);
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.checkoutFailed"));
      setPhase("form");
    }
  };

  const handleJoin = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setPhase("joining");
    try {
      const { org_id } = await redeemInvite(invite.trim());
      selectOrg(org_id);
      await refresh();
      enterOrg(org_id, "/query");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.joinFailed"));
      setPhase("form");
    }
  };

  if (phase === "welcome") {
    // Reached only when the create finished with something to report — the success path is a
    // document load to the org's own host, which is where this same page is normally read.
    return (
      <Box maw={560} mx="auto" my={80} data-testid="onboard-org-welcome">
        {error && (
          <Alert color="red" mb="lg" data-testid="onboard-welcome-error">
            {error}
          </Alert>
        )}
        <OrgWelcomePage />
      </Box>
    );
  }

  return (
    <Box maw={480} mx="auto" my={80} data-testid="onboard-org-page">
      <Group justify="space-between" align="flex-start" wrap="nowrap">
        <Title order={2}>{t("onboardOrg.title")}</Title>
        {/* The membership gate holds an org-less account on this page and there is no navbar here,
            so this is the only way off it — without it, signing in with the wrong account is a dead
            end that only clearing site data escapes. */}
        <Button
          variant="subtle"
          size="compact-sm"
          data-testid="onboard-sign-out"
          onClick={() => void signOut()}
        >
          {t("onboardOrg.signOut")}
        </Button>
      </Group>
      <Text c="dimmed" size="sm" mb="lg">
        {email ? t("onboardOrg.subtitleAs", { email }) : t("onboardOrg.subtitle")}
      </Text>

      {phase === "checkout" ? (
        <Stack gap="md" align="center" data-testid="onboard-org-checkout">
          <Loader />
          <Text>{t("onboardOrg.openingCheckout")}</Text>
        </Stack>
      ) : phase === "creating" || phase === "provisioning" ? (
        <Stack gap="md" align="center" data-testid="onboard-org-provisioning">
          <Loader />
          <Text>
            {phase === "provisioning"
              ? t("onboardOrg.finishingSetup")
              : t("onboardOrg.provisioning")}
          </Text>
        </Stack>
      ) : phase === "joining" ? (
        <Stack gap="md" align="center" data-testid="onboard-org-joining">
          <Loader />
          <Text>{t("onboardOrg.joining")}</Text>
        </Stack>
      ) : (
        <Stack gap="lg">
          {reservation && (
            <Alert
              variant="light"
              color="yellow"
              title={t("onboardOrg.resumeTitle")}
              data-testid="onboard-reservation"
            >
              <Stack gap="sm">
                <Text size="sm">
                  {t("onboardOrg.resumeBody", {
                    name: reservation.name,
                    expires: new Date(reservation.expires_at).toLocaleTimeString(),
                  })}
                </Text>
                <Group>
                  <Button
                    size="xs"
                    data-testid="onboard-resume-checkout"
                    disabled={offer === null}
                    onClick={() => offer && void handleResume(reservation, offer)}
                  >
                    {t("onboardOrg.resumeButton")}
                  </Button>
                </Group>
              </Stack>
            </Alert>
          )}
          {pendingInvites.length > 0 && (
            <Alert
              variant="light"
              color="blue"
              icon={<UserPlus size={18} />}
              title={t("onboardOrg.pendingInvitesTitle")}
              data-testid="onboard-pending-invites"
            >
              <Stack gap="sm">
                <Text size="sm">{t("onboardOrg.pendingInvitesBody")}</Text>
                {pendingInvites.map((inv) => (
                  <Group key={inv.token} justify="space-between" wrap="nowrap">
                    <Text size="sm" fw={500}>
                      {inv.org_name}
                    </Text>
                    <Button
                      size="xs"
                      data-testid={`onboard-accept-invite-${inv.org_id}`}
                      onClick={() => void acceptInvite(inv.token)}
                    >
                      {t("onboardOrg.acceptInvite")}
                    </Button>
                  </Group>
                ))}
              </Stack>
            </Alert>
          )}
          {autoJoinOffers.length > 0 && (
            <Alert
              variant="light"
              color="blue"
              icon={<UserPlus size={18} />}
              title={t("onboardOrg.autoJoinChoiceTitle")}
              data-testid="onboard-auto-join-offers"
            >
              <Stack gap="sm">
                <Text size="sm">{t("onboardOrg.autoJoinChoiceBody")}</Text>
                {autoJoinOffers.map((offered) => (
                  <Group key={offered.org_id} justify="space-between" wrap="nowrap">
                    <Text size="sm" fw={500}>
                      {offered.org_name}
                    </Text>
                    <Button
                      size="xs"
                      data-testid={`onboard-join-offered-${offered.org_id}`}
                      onClick={() => void joinOfferedOrg(offered.org_id)}
                    >
                      {t("onboardOrg.joinOfferedOrg")}
                    </Button>
                  </Group>
                ))}
                <Group justify="flex-end">
                  <Button
                    size="xs"
                    variant="subtle"
                    data-testid="onboard-decline-auto-join"
                    onClick={() => void declineOfferedOrgs()}
                  >
                    {t("onboardOrg.declineAutoJoin")}
                  </Button>
                </Group>
              </Stack>
            </Alert>
          )}
          <SegmentedControl
            fullWidth
            data-testid="onboard-org-mode"
            value={mode}
            onChange={(v) => {
              setMode(v as "create" | "join");
              setError(null);
            }}
            data={[
              { label: t("onboardOrg.modeCreate"), value: "create" },
              { label: t("onboardOrg.modeJoin"), value: "join" },
            ]}
          />

          {mode === "create" ? (
            <form onSubmit={handleCreate}>
              <Stack gap="md">
                <TextInput
                  id="onboard-org-id"
                  data-testid="onboard-org-id"
                  label={t("onboardOrg.orgIdLabel")}
                  description={t("onboardOrg.orgIdDesc")}
                  value={id}
                  onChange={(e) => setId(e.currentTarget.value)}
                  required
                  pattern="[a-z][a-z0-9]{1,39}"
                />
                <TextInput
                  id="onboard-org-name"
                  data-testid="onboard-org-name"
                  label={t("onboardOrg.orgNameLabel")}
                  value={name}
                  onChange={(e) => setName(e.currentTarget.value)}
                  required
                />
                <Checkbox
                  data-testid="onboard-org-demo"
                  label={t("onboardOrg.includeDemoLabel")}
                  description={t("onboardOrg.includeDemoDesc")}
                  checked={includeDemo}
                  onChange={(e) => setIncludeDemo(e.currentTarget.checked)}
                />
                {/* REQ-1510: where plans are sold the plan carries the lane, so the checkbox would
                    be a second, contradictable answer to the same question. */}
                {!billing && (
                  <Checkbox
                    data-testid="onboard-org-isolated-engine"
                    label={t("onboardOrg.isolatedEngineLabel")}
                    description={t("onboardOrg.isolatedEngineDesc")}
                    checked={isolatedEngine}
                    onChange={(e) => setIsolatedEngine(e.currentTarget.checked)}
                  />
                )}
                {/* REQ-1514: what each plan actually costs and caps. The checkout overlay renders a
                    variant by its first tier's unit price, which is zero on every plan, and says
                    nothing of the hourly rate, the source cap, or the transfer allowance. */}
                {plans && (
                  <Radio.Group
                    data-testid="onboard-org-plan"
                    label={t("onboardOrg.planLabel")}
                    description={t("onboardOrg.planDesc")}
                    value={plan}
                    onChange={setPlan}
                  >
                    <Stack gap="xs" mt="xs">
                      {plans.map((p) => (
                        <Paper
                          key={p.plan}
                          withBorder
                          p="sm"
                          radius="md"
                          data-testid={`onboard-org-plan-${p.plan}`}
                        >
                          <Radio
                            value={p.plan}
                            label={
                              <Text fw={600}>
                                {planName(p.plan)}
                                {p.fixed_cents !== null &&
                                  ` — ${formatMoney(p.fixed_cents)}/${p.fixed_interval}`}
                              </Text>
                            }
                          />
                          <Stack gap={2} mt={6} ml={30}>
                            {p.fixed_cents !== null && (
                              <Text size="xs" c="dimmed">
                                {t("onboardOrg.planMinimum", {
                                  amount: formatMoney(p.fixed_cents),
                                  interval: p.fixed_interval,
                                  hours: p.included_hours,
                                })}
                              </Text>
                            )}
                            <Text size="xs" c="dimmed">
                              {t("onboardOrg.planHourly", {
                                amount: formatMoney(p.hourly_cents),
                              })}
                            </Text>
                            <Text size="xs" c="dimmed">
                              {t("onboardOrg.planEgress", {
                                gb: p.egress.included_gb,
                                amount: formatMoney(p.egress.per_gb_cents),
                              })}
                            </Text>
                            <Text size="xs" c="dimmed">
                              {t("onboardOrg.planSources", { sources: p.source_limit })}
                            </Text>
                            <Text size="xs" c="dimmed">
                              {p.engine
                                ? t("onboardOrg.planEngineDedicated", {
                                    vcpu: p.engine.vcpu,
                                    memory: p.engine.memory_gib,
                                  })
                                : t("onboardOrg.planEngineShared")}
                            </Text>
                            {/* REQ-1566: a trial this account is not owed is not advertised —
                                the plan is still orderable, billed from the first invoice. */}
                            <Text
                              size="xs"
                              c={p.trial_days === null || !trialAvailable ? "dimmed" : "green"}
                            >
                              {p.trial_days === null
                                ? t("onboardOrg.planNoTrial")
                                : trialAvailable
                                  ? t("onboardOrg.planTrial", { days: p.trial_days })
                                  : t("onboardOrg.planTrialSpent")}
                            </Text>
                          </Stack>
                        </Paper>
                      ))}
                    </Stack>
                  </Radio.Group>
                )}
                <TextInput
                  id="onboard-org-email-rule"
                  data-testid="onboard-org-email-rule"
                  label={t("onboardOrg.emailRuleLabel")}
                  description={t("onboardOrg.emailRuleDesc")}
                  placeholder="@acme\.com$"
                  value={emailRule}
                  onChange={(e) => {
                    setEmailRule(e.currentTarget.value);
                    // REQ-1567: the acceptance was of what the PREVIOUS rule admitted. A new rule
                    // admits a different set, so it has to be measured and accepted again.
                    setBreadthWarning(null);
                    setRiskAcknowledged(false);
                  }}
                />
                <Checkbox
                  data-testid="onboard-org-auto-join"
                  label={t("onboardOrg.autoJoinLabel")}
                  description={t("onboardOrg.autoJoinDesc")}
                  checked={autoJoin}
                  onChange={(e) => setAutoJoin(e.currentTarget.checked)}
                />
                {autoJoin && (
                  <TextInput
                    id="onboard-org-auto-join-role"
                    data-testid="onboard-org-auto-join-role"
                    label={t("onboardOrg.autoJoinRoleLabel")}
                    description={t("onboardOrg.autoJoinRoleDesc")}
                    placeholder="analyst"
                    value={autoJoinRole}
                    onChange={(e) => setAutoJoinRole(e.currentTarget.value)}
                    required
                  />
                )}
                {breadthWarning && (
                  <Alert
                    variant="light"
                    color="yellow"
                    data-testid="onboard-org-breadth-warning"
                    title={t("onboardOrg.autoJoinBreadthTitle")}
                  >
                    <Stack gap="xs">
                      <Text size="sm">{breadthWarning}</Text>
                      <Checkbox
                        data-testid="onboard-org-accept-risk"
                        label={t("onboardOrg.autoJoinAcceptRisk")}
                        checked={riskAcknowledged}
                        onChange={(e) => setRiskAcknowledged(e.currentTarget.checked)}
                      />
                    </Stack>
                  </Alert>
                )}
                {error && (
                  <Alert variant="light" color="red" data-testid="onboard-org-error">
                    {error}
                  </Alert>
                )}
                {/* REQ-1476: on a commercial deployment this button opens a checkout — the org is
                    created by the subscription, so it says what it does. */}
                {billing && offer && (
                  <Text size="sm" c="dimmed" data-testid="onboard-org-signup-desc">
                    {offer.trial_days === null || !trialAvailable
                      ? t("onboardOrg.signUpDescPaid")
                      : t("onboardOrg.signUpDesc")}
                  </Text>
                )}
                <Button
                  type="submit"
                  data-testid="onboard-org-submit"
                  disabled={billing && offer === null}
                >
                  {billing ? t("onboardOrg.signUpButton") : t("onboardOrg.createButton")}
                </Button>
              </Stack>
            </form>
          ) : (
            <form onSubmit={handleJoin}>
              <Stack gap="md">
                <TextInput
                  id="onboard-org-invite"
                  data-testid="onboard-org-invite"
                  label={t("onboardOrg.inviteLabel")}
                  description={t("onboardOrg.inviteDesc")}
                  value={invite}
                  onChange={(e) => setInvite(e.currentTarget.value)}
                  required
                />
                {error && (
                  <Alert variant="light" color="red" data-testid="onboard-org-error">
                    {error}
                  </Alert>
                )}
                <Button type="submit" data-testid="onboard-org-join-submit">
                  {t("onboardOrg.joinButton")}
                </Button>
              </Stack>
            </form>
          )}
        </Stack>
      )}
    </Box>
  );
}
