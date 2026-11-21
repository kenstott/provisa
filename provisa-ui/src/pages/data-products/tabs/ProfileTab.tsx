// Copyright (c) 2026 Kenneth Stott
// Canary: 1f7a3c95-4d2b-4e08-8a6c-b9e5d0f3a271
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180, A PROFILE SHOWN IS SANITIZED: a data product's Profile section -- each dataset's latest
// profile as the server has sanitized it for the viewer's role, drawn by the tables page's profile
// panel (REQ-2239): in full for the product's owner, read-only for anyone else.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Loader, Text } from "@mantine/core";
import { fetchProductProfiles, type ProductProfile } from "../../../api/dataProductOffers";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";
import { SanitizedProfilePanel } from "../../tables/LatestProfilePanel";

export function ProfileTab({
  productId,
  owner,
  role,
  onLoaded,
}: {
  productId: string;
  owner: boolean;
  /** The role the profile is shown as; none acting, nothing is read. */
  role: string | null;
  /** Its profiles once read: the section's summary is computed from them. */
  onLoaded?: (profiles: ProductProfile[]) => void;
}) {
  const { t } = useTranslation();
  const [read, setRead] = useState<{ profiles: ProductProfile[] | null; error: string | null }>({
    profiles: null,
    error: null,
  });
  useEffect(() => {
    if (!role) return;
    let live = true;
    fetchProductProfiles(productId, role).then(
      (profiles) => {
        if (!live) return;
        setRead({ profiles, error: null });
        onLoaded?.(profiles);
      },
      (e: unknown) =>
        live && setRead({ profiles: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read again on these, not on a new callback.
  }, [productId, role]);

  if (read.error) {
    return (
      <Alert color="red" data-testid="data-product-profile-error">
        {read.error}
      </Alert>
    );
  }
  if (read.profiles === null)
    return <Loader size="sm" data-testid="data-product-profile-loading" />;
  if (read.profiles.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="data-product-profile-empty">
        {t("dataProductCore.profilesEmpty")}
      </Text>
    );
  }
  return (
    <Collapsibles id={`data-product.profile.${productId}`}>
      {read.profiles.map((p) => (
        <CollapsibleItem
          key={p.table_id}
          value={String(p.table_id)}
          defaultOpen={read.profiles?.length === 1}
          mountWhenOpen
          controlTestId={`data-product-profile-${p.table_id}-toggle`}
          title={
            <Text size="sm" ff="monospace">
              {p.table}
            </Text>
          }
        >
          <SanitizedProfilePanel
            profile={{
              tableId: p.table_id,
              tableName: p.table,
              run: p.run,
              results: p.results,
            }}
            mode={owner ? "owner" : "consumer"}
            testId={`data-product-profile-${p.table_id}`}
          />
        </CollapsibleItem>
      ))}
    </Collapsibles>
  );
}
