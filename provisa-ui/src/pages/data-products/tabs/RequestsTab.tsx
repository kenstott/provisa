// Copyright (c) 2026 Kenneth Stott
// Canary: 6c9e1a42-7b5f-4d38-a0e6-f2b8c4d1e957
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180, REQUESTS GO THROUGH HOOKS: a data product's Requests tab, its owner's alone -- the bids
// for its offers in the product's own request queue, each accepted or declined as bid (a decline
// states why), and who holds read on each of its datasets now (the tables page's access panel).

import { DestructiveConfirmButton } from "../../../components/form/DestructiveConfirmButton";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Group,
  Loader,
  Stack,
  Table,
  Text,
  Textarea,
  Title,
} from "@mantine/core";
import {
  acceptBid,
  declineBid,
  fetchBids,
  type Bid,
  type BidStatus,
} from "../../../api/dataProductOffers";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";
import { FormField, FormFooter, FormLayout } from "../../../components/form/FormLayout";
import { TableAccessPanel } from "../../tables/TableAccessPanel";
import { usePanelDirty } from "../../../components/list/panelLeave";
import type { OfferDataset } from "../offers/AddOfferForm";

const STATUS_COLOR: Record<BidStatus, string> = {
  pending: "yellow",
  accepted: "green",
  declined: "red",
  withdrawn: "gray",
  cancelled: "gray",
};

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function DeclineForm({
  productId,
  bid,
  onDone,
  onCancel,
}: {
  productId: string;
  bid: Bid;
  onDone: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  usePanelDirty(reason.trim() !== "", t("dataProductOffers.decline"));
  return (
    <FormLayout data-testid={`bid-${bid.id}-decline-form`}>
      <FormField size="full">
        <Textarea
          label={t("dataProductOffers.declineReason")}
          required
          autosize
          minRows={2}
          value={reason}
          onChange={(e) => setReason(e.currentTarget.value)}
          data-testid={`bid-${bid.id}-decline-reason`}
        />
      </FormField>
      <FormFooter
        onCancel={onCancel}
        problem={error}
        action={
          <DestructiveConfirmButton
            disabled={reason.trim() === ""}
            onClick={() =>
              declineBid(productId, bid.id, reason.trim()).then(onDone, (e: unknown) =>
                setError(message(e)),
              )
            }
            testId={`bid-${bid.id}-decline-send`}
          >
            {t("dataProductOffers.decline")}
          </DestructiveConfirmButton>
        }
      />
    </FormLayout>
  );
}

export function RequestsTab({
  productId,
  datasets,
  onLoaded,
}: {
  productId: string;
  datasets: OfferDataset[];
  /** Its bids each time they are read: the section's summary is computed from them. */
  onLoaded?: (bids: Bid[]) => void;
}) {
  const { t, i18n } = useTranslation();
  const [bids, setBids] = useState<Bid[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [declining, setDeclining] = useState<number | null>(null);
  const [version, setVersion] = useState(0);
  const reload = useCallback(() => setVersion((v) => v + 1), []);
  useEffect(() => {
    let live = true;
    fetchBids(productId).then(
      (b) => {
        if (!live) return;
        setBids(b);
        setError(null);
        onLoaded?.(b);
      },
      (e: unknown) => live && setError(message(e)),
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read again on these, not on a new callback.
  }, [productId, version]);

  return (
    <Stack gap="md" data-testid={`data-product-requests-${productId}`}>
      <Title order={6}>{t("dataProductOffers.bids")}</Title>
      {error && (
        <Alert color="red" data-testid="data-product-requests-error">
          {error}
        </Alert>
      )}
      {bids === null && !error && <Loader size="sm" />}
      {bids !== null && bids.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="data-product-requests-empty">
          {t("dataProductOffers.noBids")}
        </Text>
      )}
      {bids !== null && bids.length > 0 && (
        <Table.ScrollContainer minWidth={640}>
          <Table striped withTableBorder verticalSpacing="xs" fz="sm">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("dataProductOffers.requester")}</Table.Th>
                <Table.Th>{t("dataProductOffers.role")}</Table.Th>
                <Table.Th>{t("dataProductOffers.reason")}</Table.Th>
                <Table.Th>{t("dataProductOffers.status")}</Table.Th>
                <Table.Th>{t("dataProductOffers.requestedAt")}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {bids.map((bid) => (
                <Table.Tr key={bid.id} data-testid={`bid-${bid.id}`}>
                  <Table.Td>{bid.requester}</Table.Td>
                  <Table.Td ff="monospace">{bid.terms.role_id ?? ""}</Table.Td>
                  <Table.Td>
                    {bid.reason}
                    {bid.decline_reason && (
                      <Text size="xs" c="dimmed">
                        {t("dataProductOffers.declinedBecause", { reason: bid.decline_reason })}
                      </Text>
                    )}
                    {declining === bid.id && (
                      <DeclineForm
                        productId={productId}
                        bid={bid}
                        onCancel={() => setDeclining(null)}
                        onDone={() => {
                          setDeclining(null);
                          reload();
                        }}
                      />
                    )}
                  </Table.Td>
                  <Table.Td>
                    <Badge size="sm" variant="light" color={STATUS_COLOR[bid.status]}>
                      {t(`dataProductOffers.status_${bid.status}`)}
                    </Badge>
                  </Table.Td>
                  <Table.Td>{new Date(bid.created_at).toLocaleString(i18n.language)}</Table.Td>
                  <Table.Td>
                    {bid.status === "pending" && declining !== bid.id && (
                      <Group gap={4} wrap="nowrap">
                        <Button
                          size="compact-xs"
                          onClick={() =>
                            acceptBid(productId, bid.id).then(reload, (e: unknown) =>
                              setError(message(e)),
                            )
                          }
                          data-testid={`bid-${bid.id}-accept`}
                        >
                          {t("dataProductOffers.accept")}
                        </Button>
                        <Button
                          size="compact-xs"
                          variant="subtle"
                          color="red"
                          onClick={() => setDeclining(bid.id)}
                          data-testid={`bid-${bid.id}-decline`}
                        >
                          {t("dataProductOffers.decline")}
                        </Button>
                      </Group>
                    )}
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
      <Title order={6}>{t("dataProductOffers.holders")}</Title>
      <Collapsibles id={`data-product.holders.${productId}`}>
        {datasets.map((d) => (
          <CollapsibleItem
            key={d.id}
            value={String(d.id)}
            defaultOpen={false}
            mountWhenOpen
            controlTestId={`data-product-holders-${d.id}-toggle`}
            title={
              <Text size="sm" ff="monospace">
                {d.name}
              </Text>
            }
          >
            <TableAccessPanel tableId={d.id} />
          </CollapsibleItem>
        ))}
      </Collapsibles>
    </Stack>
  );
}
