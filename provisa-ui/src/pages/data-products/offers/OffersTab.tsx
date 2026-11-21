// Copyright (c) 2026 Kenneth Stott
// Canary: 2a6f9d14-8b3e-4c71-a5d2-e0c7b9f1a348
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS: a product's Offers section -- its offers as cards, and, for whoever may edit the
// product, the panel that adds one. The same section in the read view and the edit view.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Loader, Paper, Stack, Text, Title } from "@mantine/core";
import { Plus } from "lucide-react";
import { fetchOffers, type Offer } from "../../../api/dataProductOffers";
import { AddOfferForm, type OfferDataset } from "./AddOfferForm";
import { OfferCard } from "./OfferCard";
import type { ContractContext } from "./OfferContracts";

export function OffersTab({
  productId,
  owner,
  canEdit,
  datasets,
  views,
  onShowQuality,
  onPreview,
  onQueryNow,
  onBid,
  context = null,
  version: readAgain = 0,
  onLoaded,
}: {
  productId: string;
  owner: boolean;
  canEdit: boolean;
  datasets: OfferDataset[];
  views: OfferDataset[];
  onShowQuality: () => void;
  onPreview: (tableId: number) => void;
  onQueryNow: () => void;
  /** A bid was made: the Requests a reader sees have changed. */
  onBid: () => void;
  /** What each contract's heading and document read (the product's datasets, lineage, reader). */
  context?: ContractContext | null;
  /** Bumped when the product's datasets can have changed: its offers and contracts are read again. */
  version?: number;
  /** Its offers, each time they are read: the section's summary is computed from them. */
  onLoaded?: (offers: Offer[]) => void;
}) {
  const { t } = useTranslation();
  const [offers, setOffers] = useState<Offer[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [version, setVersion] = useState(0);
  const reload = useCallback(() => setVersion((v) => v + 1), []);

  useEffect(() => {
    let live = true;
    fetchOffers(productId).then(
      (o) => {
        if (!live) return;
        setOffers(o);
        setError(null);
        onLoaded?.(o);
      },
      (e: unknown) => live && setError(e instanceof Error ? e.message : String(e)),
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read again on these, not on a new callback.
  }, [productId, version, readAgain]);

  const viewName = (id: number) => views.find((v) => v.id === id)?.name ?? String(id);

  return (
    <Stack gap="sm" data-testid={`data-product-offers-${productId}`}>
      {error && (
        <Alert color="red" data-testid="data-product-offers-error">
          {error}
        </Alert>
      )}
      {offers === null && !error && <Loader size="sm" data-testid="data-product-offers-loading" />}
      {offers !== null && offers.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="data-product-offers-empty">
          {t("dataProductOffers.none")}
        </Text>
      )}
      {offers?.map((offer) => (
        <OfferCard
          key={offer.id}
          offer={offer}
          productId={productId}
          owner={owner}
          onRemoved={reload}
          onBid={() => {
            reload();
            onBid();
          }}
          onShowQuality={onShowQuality}
          onPreview={onPreview}
          onQueryNow={onQueryNow}
          viewName={viewName}
          context={context}
        />
      ))}
      {canEdit &&
        (adding ? (
          <Paper withBorder p="sm" data-testid="add-offer-panel">
            <Title order={6} mb="xs">
              {t("dataProductOffers.addTitle")}
            </Title>
            <AddOfferForm
              productId={productId}
              datasets={datasets}
              views={views}
              onCancel={() => setAdding(false)}
              onSaved={() => {
                setAdding(false);
                reload();
              }}
            />
          </Paper>
        ) : (
          <div>
            <Button
              size="xs"
              variant="light"
              leftSection={<Plus size={13} />}
              onClick={() => setAdding(true)}
              data-testid="add-offer-open-button"
            >
              {t("dataProductOffers.add")}
            </Button>
          </div>
        ))}
    </Stack>
  );
}
