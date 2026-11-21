// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7d4e61-93a2-4f1c-8d5e-2c6a9f13b7e4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: a dbt source's public models with enforced contracts, registered all at once from
// Register Table as draft data products, after a preview: one per dbt group, or one for a project
// with no groups.

import { Text } from "@mantine/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import {
  fetchDbtProductsPreview,
  messageOf,
  type DbtPlannedProduct,
  type DbtProductsPreview,
} from "./dbtProducts";

function ProductPreview({ product }: { product: DbtPlannedProduct }) {
  const { t } = useTranslation();
  const owner = product.owner?.name ?? product.owner?.email ?? null;
  return (
    <FormField size="full">
      <div className="dbt-product-preview" data-testid="dbt-product">
        <Text fw={600} size="sm" data-testid="dbt-product-id">
          {product.group === null
            ? t("dbtPublicProducts.projectProduct", { id: product.id })
            : t("dbtPublicProducts.groupProduct", { id: product.id })}
        </Text>
        {product.exists && (
          <Text c="red" size="sm" data-testid="dbt-product-exists">
            {t("dbtPublicProducts.exists", { id: product.id })}
          </Text>
        )}
        <Text size="sm" c="dimmed">
          {owner && (
            <>
              {t("dbtPublicProducts.owner", { owner })}
              {product.owner_role &&
                ` · ${t("dbtPublicProducts.ownerRole", { role: product.owner_role })}`}
              <br />
            </>
          )}
          {product.tests.length > 0 &&
            t("dbtPublicProducts.tests", { tests: product.tests.join(", ") })}
        </Text>
        <ul className="dbt-product-models">
          {product.models.map((m) => (
            <li key={m.unique_id} data-testid="dbt-product-model">
              <Text span size="sm" fw={500}>
                {m.schema}.{m.relation}
              </Text>
              <Text span size="sm" c="dimmed">
                {" — "}
                {m.version && `${t("dbtPublicProducts.version", { version: m.version })} · `}
                {t("dbtPublicProducts.columns", {
                  columns: m.columns
                    .map((c) =>
                      c.required ? `${c.name} (${t("dbtPublicProducts.required")})` : c.name,
                    )
                    .join(", "),
                })}
                {m.table_id !== null && ` · ${t("dbtPublicProducts.alreadyRegistered")}`}
              </Text>
            </li>
          ))}
        </ul>
      </div>
    </FormField>
  );
}

interface Props {
  sourceId: string;
  /** The preview of ``sourceId``, once read; the form confirms it. */
  onPreview: (sourceId: string, preview: DbtProductsPreview) => void;
}

/** What registering the source's public models would register and create. Nothing is created by
 *  showing it. Keyed by its source, so another source starts with no preview. */
export function DbtPublicProducts({ sourceId, onPreview }: Props) {
  const { t } = useTranslation();
  const [preview, setPreview] = useState<DbtProductsPreview | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let current = true;
    fetchDbtProductsPreview(sourceId)
      .then((held) => {
        if (!current) return;
        setPreview(held);
        onPreview(sourceId, held);
      })
      .catch((err: unknown) => {
        if (current) setError(messageOf(err));
      });
    return () => {
      current = false;
    };
  }, [sourceId, onPreview]);

  return (
    <>
      <FormField size="full">
        {error && (
          <Text c="red" size="sm" data-testid="dbt-products-error">
            {error}
          </Text>
        )}
        {preview === null && !error && (
          <Text size="sm" c="dimmed">
            {t("common.loading")}
          </Text>
        )}
        {preview?.products.length === 0 && (
          <Text size="sm" c="dimmed" data-testid="dbt-products-none">
            {t("dbtPublicProducts.none")}
          </Text>
        )}
        {preview && preview.products.length > 0 && (
          <Text size="sm" c="dimmed" data-testid="dbt-products-intro">
            {t("dbtPublicProducts.intro", { count: preview.products.length })}
          </Text>
        )}
      </FormField>
      {preview?.products.map((p) => (
        <ProductPreview key={p.id} product={p} />
      ))}
      {preview && preview.ungrouped.length > 0 && (
        <FormField size="full">
          <Text size="sm" c="dimmed" data-testid="dbt-products-ungrouped">
            {t("dbtPublicProducts.ungrouped", { models: preview.ungrouped.join(", ") })}
          </Text>
        </FormField>
      )}
    </>
  );
}
