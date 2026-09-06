// Copyright (c) 2026 Kenneth Stott
// Canary: 8f1d5a2e-6c3b-4e97-9a1d-3b7c6e0a5f42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Group, Select, Stack, Table, TextInput, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Package, Plus, Trash2 } from "lucide-react";
import { useDataProducts, useCreateDataProduct, useDeleteDataProduct, useDomains } from "../../hooks/useAdminQueries";
import { FilterInput } from "./FilterInput";
import { HelpBubble } from "../HelpBubble";

// REQ-1634: one org-level DataProduct registry, each row scoped to exactly one domain — a
// table may only join a product whose domain_id matches its own (enforced server-side).
export function DataProductsTab() {
  const { t } = useTranslation();
  const { dataProducts } = useDataProducts();
  const { domains } = useDomains();
  const { createDataProduct } = useCreateDataProduct();
  const { deleteDataProduct } = useDeleteDataProduct();

  const [search, setSearch] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [formId, setFormId] = useState("");
  const [formDomainId, setFormDomainId] = useState<string | null>(null);
  const [formName, setFormName] = useState("");
  const [formOwner, setFormOwner] = useState("");
  const [formDescription, setFormDescription] = useState("");

  const domainOptions = domains
    .filter((d) => d.id !== "")
    .map((d) => ({ label: d.id, value: d.id }));

  const q = search.toLowerCase();
  const filtered = dataProducts.filter(
    (p) =>
      p.id.toLowerCase().includes(q) ||
      p.name.toLowerCase().includes(q) ||
      p.domainId.toLowerCase().includes(q),
  );

  const resetForm = () => {
    setFormId("");
    setFormDomainId(null);
    setFormName("");
    setFormOwner("");
    setFormDescription("");
  };

  const openCreate = () => {
    if (showForm) {
      setShowForm(false);
      resetForm();
      return;
    }
    resetForm();
    setShowForm(true);
  };

  const handleSubmit = async () => {
    if (!formId.trim() || !formDomainId || !formName.trim()) return;
    const result = await createDataProduct(
      formId.trim(),
      formDomainId,
      formName.trim(),
      formOwner.trim() || null,
      formDescription.trim(),
    );
    if (result.success) {
      notifications.show({ color: "green", message: t("dataProductsTab.created", { id: formId.trim() }) });
      setShowForm(false);
      resetForm();
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  const handleDelete = async (id: string) => {
    if (!window.confirm(t("dataProductsTab.deleteConfirm", { id }))) return;
    const result = await deleteDataProduct(id);
    if (result.success) {
      notifications.show({ message: t("dataProductsTab.deleted", { id }) });
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  return (
    <Stack gap="md">
      <Group justify="space-between" wrap="wrap">
        <Group gap="xs">
          <Package size={18} />
          <Title order={3}>{t("dataProductsTab.heading")}</Title>
        </Group>
        <FilterInput
          value={search}
          onChange={setSearch}
          placeholder={t("dataProductsTab.filterPlaceholder")}
        />
        <Group gap="xs">
          <Button
            variant={showForm ? "default" : "filled"}
            leftSection={<Plus size={14} />}
            onClick={openCreate}
            data-testid="data-products-create-button"
          >
            {showForm ? t("dataProductsTab.closeForm") : t("dataProductsTab.addDataProduct")}
          </Button>
          <HelpBubble
            title={t("dataProductsTab.purposeTitle")}
            paragraphs={[t("dataProductsTab.purposeBody")]}
            ariaLabel={t("dataProductsTab.purposeAria")}
            testId="data-products-purpose-help"
          />
        </Group>
      </Group>

      {showForm && (
        <Stack gap="sm" maw={480}>
          <TextInput
            label={t("dataProductsTab.idLabel")}
            placeholder={t("dataProductsTab.idPlaceholder")}
            value={formId}
            onChange={(e) => setFormId(e.currentTarget.value)}
            data-testid="data-products-new-id"
          />
          <Select
            label={t("dataProductsTab.domainLabel")}
            data={domainOptions}
            value={formDomainId}
            onChange={setFormDomainId}
            data-testid="data-products-new-domain"
          />
          <TextInput
            label={t("dataProductsTab.nameLabel")}
            placeholder={t("dataProductsTab.namePlaceholder")}
            value={formName}
            onChange={(e) => setFormName(e.currentTarget.value)}
            data-testid="data-products-new-name"
          />
          <TextInput
            label={t("dataProductsTab.ownerLabel")}
            placeholder={t("dataProductsTab.ownerPlaceholder")}
            value={formOwner}
            onChange={(e) => setFormOwner(e.currentTarget.value)}
            data-testid="data-products-new-owner"
          />
          <TextInput
            label={t("dataProductsTab.descriptionLabel")}
            placeholder={t("dataProductsTab.descriptionPlaceholder")}
            value={formDescription}
            onChange={(e) => setFormDescription(e.currentTarget.value)}
            data-testid="data-products-new-description"
          />
          <Button
            onClick={handleSubmit}
            disabled={!formId.trim() || !formDomainId || !formName.trim()}
            style={{ alignSelf: "flex-start" }}
            data-testid="data-products-submit"
          >
            {t("dataProductsTab.createButton")}
          </Button>
        </Stack>
      )}

      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("dataProductsTab.colId")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colDomain")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colName")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colOwner")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colDescription")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colActions")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filtered.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={6} ta="center" c="dimmed">
                  {t("dataProductsTab.empty")}
                </Table.Td>
              </Table.Tr>
            )}
            {filtered.map((p) => (
              <Table.Tr key={p.id} data-testid={`data-products-row-${p.id}`}>
                <Table.Td>{p.id}</Table.Td>
                <Table.Td>{p.domainId}</Table.Td>
                <Table.Td>{p.name}</Table.Td>
                <Table.Td>{p.owner || "—"}</Table.Td>
                <Table.Td>{p.description || "—"}</Table.Td>
                <Table.Td>
                  <ActionIcon
                    variant="subtle"
                    color="red"
                    aria-label={t("dataProductsTab.deleteDataProduct", { id: p.id })}
                    data-testid={`data-products-delete-${p.id}`}
                    onClick={() => handleDelete(p.id)}
                  >
                    <Trash2 size={14} />
                  </ActionIcon>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}
