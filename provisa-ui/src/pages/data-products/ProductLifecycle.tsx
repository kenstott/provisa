// Copyright (c) 2026 Kenneth Stott
// Canary: 266137db-8895-414e-8d67-636bd21b9228
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634 STATUS, REQ-2146: a data product's lifecycle on its panel -- its status and why, the
// actions that change it (publish a draft; deprecate a live product; retire a live or deprecated
// one, its members moving to a successor; restore either), and its findings with their fixes.
// Built from the lifecycle pieces the glossary's terms use.

import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Button, Text } from "@mantine/core";
import { Archive, ArchiveRestore, Hourglass, Send } from "lucide-react";
import type { DataProduct, MutationResult } from "../../types/admin";
import type { DetailField } from "../../components/detail/DetailFields";
import { ProductStatusBadge } from "./ProductStatusBadge";
import { DeprecateDialog } from "../../components/lifecycle/DeprecateDialog";
import { RetireDialog } from "../../components/lifecycle/RetireDialog";
import { ToReview, type FindingFix } from "../../components/lifecycle/ToReview";
import { Tip } from "../../components/Tip";
import { refusalError, serverMessage } from "../../i18n/serverMessage";
import { entityHref } from "../../lib/entityHref";
import type { DataProductStatus, ProductFinding } from "../../hooks/useDataProductMutations";

/** What the panel draws of a product's lifecycle. */
export interface ProductLifecycleView {
  /** Its status, why, its planned retirement and its successor, for the details card. */
  fields: DetailField[];
  /** The status actions the caller may take; null when it may take none. */
  actions: ReactNode;
  /** Its findings with their fixes; nothing when it has none. */
  toReview: ReactNode;
  /** The deprecate and retire dialogs, while one is open. */
  dialogs: ReactNode;
}

export function useProductLifecycle({
  product,
  products,
  members,
  findings,
  canEdit,
  onChanged,
  onFailed,
  onEdit,
  openProduct,
  status,
}: {
  product: DataProduct;
  /** Every product the role reads: the successors to choose from, and the successor's name. */
  products: DataProduct[];
  /** The names of its tables, views and commands: what a retirement moves to the successor. */
  members: string[];
  findings: ProductFinding[];
  canEdit: boolean;
  /** A change was made: the products are read again, and ``message`` told. */
  onChanged: (message: string) => void;
  onFailed: (message: string) => void;
  onEdit: (focus?: "supportContact") => void;
  openProduct: (id: string) => void;
  /** The status actions, as the server takes them. */
  status: DataProductStatus;
}): ProductLifecycleView {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [deprecateOpen, setDeprecateOpen] = useState(false);
  const [retireOpen, setRetireOpen] = useState(false);
  const id = product.id;

  // The success the server names by code, in the reader's language.
  const done = (code: string) =>
    serverMessage({ code: `data_product.status.${code}`, params: { data_product: id } }, "");
  // A refusal throws, so a dialog shows it in place.
  const refusing = async (answer: Promise<MutationResult>) => {
    const r = await answer;
    if (!r.success) throw refusalError(r);
    return r;
  };
  const act = async (answer: Promise<MutationResult>) => {
    const r = await answer;
    if (r.success) onChanged(serverMessage(r, r.message));
    else onFailed(serverMessage(r, r.message));
  };

  const successor = products.find((p) => p.id === product.successorId);
  const fields: DetailField[] = [
    {
      label: t("dataProductsTab.statusLabel"),
      value: <ProductStatusBadge status={product.status} testId={`data-product-status-${id}`} />,
    },
    ...(product.statusReason
      ? [{ label: t("dataProductsTab.statusReasonLabel"), value: product.statusReason }]
      : []),
    ...(product.retireOn
      ? [{ label: t("dataProductsTab.retireOnLabel"), value: product.retireOn }]
      : []),
    ...(product.successorId
      ? [
          {
            label: t("dataProductsTab.successorLabel"),
            value: (
              <Button
                variant="subtle"
                size="compact-sm"
                px={0}
                onClick={() => openProduct(product.successorId as string)}
                data-testid={`data-product-successor-${id}`}
              >
                {successor ? successor.name : product.successorId}
              </Button>
            ),
          },
        ]
      : []),
  ];

  const s = product.status;
  const actions = canEdit ? (
    <>
      {s === "draft" && (
        <Tip text={t("dataProductsTab.publishHint")}>
          <Button
            size="xs"
            variant="light"
            color="green"
            leftSection={<Send size={14} />}
            onClick={() => void act(status.publish(id))}
            data-testid={`data-product-publish-${id}`}
          >
            {t("dataProductsTab.publish")}
          </Button>
        </Tip>
      )}
      {s === "live" && (
        <Tip text={t("dataProductsTab.deprecateHint")}>
          <Button
            size="xs"
            variant="light"
            color="gray"
            leftSection={<Hourglass size={14} />}
            onClick={() => setDeprecateOpen(true)}
            data-testid={`data-product-deprecate-${id}`}
          >
            {t("dataProductsTab.deprecate")}
          </Button>
        </Tip>
      )}
      {(s === "live" || s === "deprecated") && (
        <Tip text={t("dataProductsTab.retireHint")}>
          <Button
            size="xs"
            variant="light"
            color="orange"
            leftSection={<Archive size={14} />}
            onClick={() => setRetireOpen(true)}
            data-testid={`data-product-retire-${id}`}
          >
            {t("dataProductsTab.retire")}
          </Button>
        </Tip>
      )}
      {(s === "deprecated" || s === "retired") && (
        <Tip text={t("dataProductsTab.restoreHint")}>
          <Button
            size="xs"
            variant="light"
            color="teal"
            leftSection={<ArchiveRestore size={14} />}
            onClick={() => void act(status.restore(id))}
            data-testid={`data-product-restore-${id}`}
          >
            {t("dataProductsTab.restore")}
          </Button>
        </Tip>
      )}
    </>
  ) : null;

  const edit: FindingFix = {
    label: t("dataProductsTab.findingFix_edit"),
    run: () => onEdit(),
    testId: `data-product-finding-fix-edit-${id}`,
  };
  const fixesOf = (f: ProductFinding): FindingFix[] => {
    switch (f.kind) {
      case "member_deprecated":
        return [
          {
            label: t("dataProductsTab.findingFix_member_deprecated", { table: f.params.table }),
            run: () => navigate(entityHref("table", f.params.table_id)),
            testId: `data-product-finding-fix-table-${id}`,
          },
        ];
      case "domain_steward_unset":
        return [
          {
            label: t("dataProductsTab.findingFix_domain_steward_unset", {
              domain: f.params.domain,
            }),
            run: () => navigate(entityHref("domain", f.params.domain)),
            testId: `data-product-finding-fix-domain-${id}`,
          },
        ];
      case "successor_retired":
        return [
          {
            label: t("dataProductsTab.findingFix_successor_retired", {
              successor: f.params.successor,
            }),
            run: () => openProduct(f.params.successor),
            testId: `data-product-finding-fix-successor-${id}`,
          },
        ];
      case "retirement_blocked":
        return canEdit
          ? [
              {
                label: t("dataProductsTab.findingFix_retirement_blocked"),
                run: () => setRetireOpen(true),
                testId: `data-product-finding-fix-retire-${id}`,
              },
            ]
          : [];
      case "no_support_contact":
        // The form opens on the support contact.
        return canEdit ? [{ ...edit, run: () => onEdit("supportContact") }] : [];
      case "no_members":
      case "no_owner":
      case "no_description":
        return canEdit ? [edit] : [];
      default:
        return [];
    }
  };
  const toReview = (
    <ToReview
      title={t("dataProductsTab.toReview")}
      findings={findings}
      chipLabel={(f) => t(`dataProductsTab.finding_${f.kind}`)}
      fixesOf={fixesOf}
      testId={`data-product-${id}`}
    />
  );

  const dialogs = (
    <>
      {deprecateOpen && (
        <DeprecateDialog
          text={{
            title: t("dataProductsTab.deprecateTitle", { product: product.name }),
            reason: t("dataProductsTab.reasonLabel"),
            retireOn: t("dataProductsTab.retireOnLabel"),
            retireOnHint: t("dataProductsTab.retireOnHint"),
            submit: t("dataProductsTab.deprecate"),
          }}
          deprecate={(reason, retireOn) => refusing(status.deprecate(id, reason, retireOn))}
          onClose={() => setDeprecateOpen(false)}
          onDeprecated={() => {
            setDeprecateOpen(false);
            onChanged(done("deprecated"));
          }}
          testId="data-product"
        />
      )}
      {retireOpen && (
        <RetireDialog
          text={{
            title: t("dataProductsTab.retireTitle", { product: product.name }),
            reason: t("dataProductsTab.reasonLabel"),
            successor: t("dataProductsTab.successorLabel"),
            successorHint: t("dataProductsTab.successorHint"),
            submit: t("dataProductsTab.retire"),
          }}
          // A successor shares the product's domain, and is in service.
          successorOptions={products
            .filter((p) => p.id !== id && p.domainId === product.domainId && p.status !== "retired")
            .map((p) => ({ value: p.id, label: `${p.name} (${p.id})` }))}
          successorRequired={members.length > 0}
          moves={
            <Text size="sm" c="dimmed">
              {members.length > 0
                ? t("dataProductsTab.retireMoves", { members: members.join(", ") })
                : t("dataProductsTab.retireNoMembers")}
            </Text>
          }
          retire={(reason, successor) => refusing(status.retire(id, reason, successor))}
          onClose={() => setRetireOpen(false)}
          onRetired={() => {
            setRetireOpen(false);
            onChanged(done("retired"));
          }}
          testId="data-product"
        />
      )}
    </>
  );

  return { fields, actions, toReview, dialogs };
}
