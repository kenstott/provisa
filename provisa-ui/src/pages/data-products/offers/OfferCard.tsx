// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8a1c93-6e2d-4b7a-9d05-c3e1f7a2b856
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS, THE UI SHOWS CONTRACTS PER OFFER: one offer of a data product, as a card that
// opens to its role, its data level and duration, and the contract it gives on each dataset -- the
// columns as its role sees them (masked ones marked), its row rules in words, its sample views,
// its quality promises and profiles, and its terms of use. A requester bids for it, or queries it
// now when it is open. REQ-2239 (amended 2026-11-11): a contract's columns are the tables page's
// columns table. REQ-2180 (amended 2026-11-11, A BID NAMES ITS CONSUMER AND AGREES TO THE TERMS):
// the bid form names who approves it, asks the kind of consumer and the purpose (prefilled from the
// offer's usage), shows the terms of use, and sends only once they are agreed to.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Checkbox,
  Divider,
  Group,
  Loader,
  Paper,
  Select,
  Stack,
  Text,
  Textarea,
} from "@mantine/core";
import { Trash2 } from "lucide-react";
import {
  bidForOffer,
  fetchApprover,
  removeOffer,
  type Approver,
  type ConsumerKind,
  type Offer,
} from "../../../api/dataProductOffers";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import {
  FormChecks,
  FormField,
  FormFooter,
  FormLayout,
  ReadRow,
} from "../../../components/form/FormLayout";
import { Tip } from "../../../components/Tip";
import { usePanelDirty } from "../../../components/list/panelLeave";
import { CONSUMER_KINDS, offerSummary, TERMS_OF_USE } from "./offerTerms";
import { OfferContracts, type ContractContext } from "./OfferContracts";
import { TermsOfUse } from "./TermsOfUse";

export interface OfferCardProps {
  offer: Offer;
  productId: string;
  /** The reader owns the product: they may remove the offer, and they do not bid. */
  owner: boolean;
  onRemoved: () => void;
  onBid: () => void;
  /** Opens the Quality tab, where the promised checks and their results are. */
  onShowQuality: () => void;
  /** Looks at a dataset's (or a sample view's) rows. */
  onPreview: (tableId: number) => void;
  /** Opens the product's data in the query editor. */
  onQueryNow: () => void;
  /** Sample views by table id, as the model names them. */
  viewName: (tableId: number) => string;
  /** What each contract's heading and document read; null where it is not given. */
  context?: ContractContext | null;
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** Who approves the request, in words. */
function approverWords(approver: Approver, t: TFunction): string {
  if (approver.via === "hook") return t("dataProductOffers.approverHook", { host: approver.name });
  return approver.name
    ? t("dataProductOffers.approverOwner", { role: approver.name })
    : t("dataProductOffers.approverNone");
}

/** The request, asked inline: who approves it, the kind of consumer it is for, its purpose
 *  (prefilled from the offer's usage), and the offer's terms of use, which it agrees to. */
export function BidForm({
  productId,
  offer,
  onDone,
  onCancel,
}: {
  productId: string;
  offer: Offer;
  onDone: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [approver, setApprover] = useState<Approver | null>(null);
  const [consumerKind, setConsumerKind] = useState<ConsumerKind | null>(null);
  const [purpose, setPurpose] = useState(offer.usage);
  const [agreed, setAgreed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const testId = `offer-${offer.id}-bid`;
  usePanelDirty(
    purpose !== offer.usage || consumerKind !== null || agreed,
    t("dataProductOffers.sendBid"),
  );
  useEffect(() => {
    let live = true;
    fetchApprover(productId).then(
      (found) => live && setApprover(found),
      (e: unknown) => live && setError(message(e)),
    );
    return () => {
      live = false;
    };
  }, [productId]);
  const ready = approver !== null && consumerKind !== null && purpose.trim() !== "" && agreed;
  const submit = () =>
    bidForOffer(productId, offer.id, {
      reason: purpose.trim(),
      consumer_kind: consumerKind as ConsumerKind,
      agreed,
    }).then(onDone, (e: unknown) => setError(message(e)));
  return (
    <FormLayout data-testid={`${testId}-form`}>
      {approver === null ? (
        !error && <Loader size="sm" data-testid={`${testId}-approver-loading`} />
      ) : (
        <ReadRow
          label={t("dataProductOffers.approver")}
          value={approverWords(approver, t)}
          size="long"
          testId={`${testId}-approver`}
        />
      )}
      <FormField size="medium">
        <Select
          label={t("dataProductOffers.consumerKind")}
          required
          allowDeselect={false}
          data={CONSUMER_KINDS.map((kind) => ({
            value: kind,
            label: t(`dataProductOffers.consumerKind_${kind}`),
          }))}
          value={consumerKind}
          onChange={(v) => setConsumerKind(v as ConsumerKind | null)}
          data-testid={`${testId}-consumer-kind`}
        />
      </FormField>
      <FormField size="full" hint={t("dataProductOffers.bidPurposeHint")}>
        <Textarea
          label={t("dataProductOffers.bidPurpose")}
          required
          autosize
          minRows={2}
          value={purpose}
          onChange={(e) => setPurpose(e.currentTarget.value)}
          data-testid={`${testId}-purpose`}
        />
      </FormField>
      <FormField size="full">
        <Divider label={t("dataProductOffers.termsOfUse")} labelPosition="left" />
      </FormField>
      {TERMS_OF_USE.map((term) => (
        <ReadRow
          key={term}
          label={t(`dataProductOffers.term_${term}`)}
          value={offer[term]}
          size="full"
          testId={`${testId}-term-${term}`}
        />
      ))}
      <FormChecks>
        <FormField size="long">
          <Checkbox
            label={t("dataProductOffers.agreeToTerms")}
            checked={agreed}
            onChange={(e) => setAgreed(e.currentTarget.checked)}
            data-testid={`${testId}-agree`}
          />
        </FormField>
      </FormChecks>
      <FormFooter
        onCancel={onCancel}
        problem={error}
        action={
          <Button onClick={submit} disabled={!ready} data-testid={`${testId}-send`}>
            {t("dataProductOffers.sendBid")}
          </Button>
        }
      />
    </FormLayout>
  );
}

export function OfferCard({
  offer,
  productId,
  owner,
  onRemoved,
  onBid,
  onShowQuality,
  onPreview,
  onQueryNow,
  viewName,
  context = null,
}: OfferCardProps) {
  const { t } = useTranslation();
  const [bidding, setBidding] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const testId = `offer-card-${offer.id}`;
  const checks = offer.contracts.reduce((n, c) => n + c.contract.terms.checks.length, 0);
  const masked = offer.contracts.reduce(
    (n, c) => n + c.contract.terms.fields.filter((f) => f.masked).length,
    0,
  );
  const rowRules = offer.contracts.reduce((n, c) => n + c.contract.terms.row_rules.length, 0);
  return (
    <Paper withBorder p="sm" data-testid={testId}>
      <Group justify="space-between" align="flex-start" wrap="nowrap" gap="xs">
        <Stack gap={4} style={{ minWidth: 0 }}>
          <Group gap={6} wrap="wrap">
            <Text fw={600} size="sm" data-testid={`${testId}-name`}>
              {offer.name}
            </Text>
            <Badge size="sm" variant="light" ff="monospace" data-testid={`${testId}-role`}>
              {offer.role_id}
            </Badge>
            <Badge size="sm" variant="outline" data-testid={`${testId}-level`}>
              {t(`dataProductOffers.level_${offer.data_level}`)}
            </Badge>
            <Badge size="sm" variant="outline" color="gray" data-testid={`${testId}-duration`}>
              {offer.duration_days == null
                ? t("dataProductOffers.noEnd")
                : t("dataProductOffers.forDays", { count: offer.duration_days })}
            </Badge>
            {offer.open && (
              <Badge size="sm" color="green" variant="light" data-testid={`${testId}-open`}>
                {t("dataProductOffers.open")}
              </Badge>
            )}
            {offer.review && (
              <Tip text={offer.review}>
                <Badge size="sm" color="yellow" variant="light" data-testid={`${testId}-review`}>
                  {t("dataProductOffers.inReview")}
                </Badge>
              </Tip>
            )}
          </Group>
          <Text size="xs" c="dimmed" data-testid={`${testId}-summary`}>
            {offerSummary(offer, offer.contracts.length, t)}
          </Text>
          <Text size="xs" c="dimmed" data-testid={`${testId}-access`}>
            {t("dataProductOffers.accessLine", { masked, rowRules })}
          </Text>
          <TermsOfUse terms={offer} testId={`${testId}-terms`} />
          {offer.sample_views.length > 0 && (
            <Text size="xs" c="dimmed" data-testid={`${testId}-sample-views`}>
              {t("dataProductOffers.sampleViews", {
                views: offer.sample_views.map(viewName).join(", "),
              })}
            </Text>
          )}
          {/* The quality line: what is promised, whether results are included, and where. */}
          <Text size="xs" data-testid={`${testId}-quality`}>
            {t("dataProductOffers.qualityLine", { count: checks })}{" "}
            {offer.dq_results
              ? t("dataProductOffers.resultsIncluded")
              : t("dataProductOffers.resultsNotIncluded")}{" "}
            <Anchor
              component="button"
              type="button"
              size="xs"
              onClick={onShowQuality}
              data-testid={`${testId}-quality-link`}
            >
              {t("dataProductOffers.seeQuality")}
            </Anchor>
          </Text>
          {!owner && offer.summary.extension && (
            <Text size="xs" c="blue" data-testid={`${testId}-extension`}>
              {offer.summary.extension}
            </Text>
          )}
        </Stack>
        <Group gap="xs" wrap="nowrap">
          {!owner &&
            (offer.query_now ? (
              <Button size="xs" onClick={onQueryNow} data-testid={`${testId}-query-now`}>
                {t("dataProductOffers.queryNow")}
              </Button>
            ) : (
              !bidding && (
                <Button size="xs" onClick={() => setBidding(true)} data-testid={`${testId}-bid`}>
                  {t("dataProductOffers.bid")}
                </Button>
              )
            ))}
          {owner && (
            <ConfirmDialog
              title={t("dataProductOffers.removeTitle")}
              consequence={t("dataProductOffers.removeConsequence", { name: offer.name })}
              confirmLabel={t("common.delete")}
              testId={`${testId}-remove-dialog`}
              onConfirm={() =>
                removeOffer(productId, offer.id).then(onRemoved, (e: unknown) =>
                  setError(message(e)),
                )
              }
            >
              {(open) => (
                <Tip text={t("dataProductOffers.remove", { name: offer.name })}>
                  <Button
                    size="compact-xs"
                    variant="subtle"
                    color="red"
                    aria-label={t("dataProductOffers.remove", { name: offer.name })}
                    onClick={open}
                    data-testid={`${testId}-remove`}
                  >
                    <Trash2 size={14} />
                  </Button>
                </Tip>
              )}
            </ConfirmDialog>
          )}
        </Group>
      </Group>
      {error && (
        <Alert color="red" mt="xs" data-testid={`${testId}-error`}>
          {error}
        </Alert>
      )}
      {bidding && (
        <BidForm
          productId={productId}
          offer={offer}
          onCancel={() => setBidding(false)}
          onDone={() => {
            setBidding(false);
            onBid();
          }}
        />
      )}
      <Collapsibles id={`data-product.offer.${productId}.${offer.id}`}>
        <CollapsibleItem
          value="contracts"
          defaultOpen={false}
          mountWhenOpen
          controlTestId={`${testId}-toggle`}
          title={
            <Text size="sm" fw={500}>
              {t("dataProductOffers.contracts", { count: offer.contracts.length })}
            </Text>
          }
        >
          <OfferContracts
            productId={productId}
            offer={offer}
            context={context}
            onPreview={onPreview}
          />
        </CollapsibleItem>
      </Collapsibles>
    </Paper>
  );
}
