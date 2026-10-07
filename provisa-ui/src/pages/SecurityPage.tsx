// Copyright (c) 2026 Kenneth Stott
// Canary: 09cc2288-9f68-4d9b-914e-1ba0f0e346d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect, useCallback } from "react";
import { useNavPayload } from "../hooks/useNavPayload";
import { useTranslation } from "react-i18next";
import { Trash2, Pencil, Check, X } from "lucide-react";
import {
  Alert,
  Button,
  Group,
  ActionIcon,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { FilterInput } from "../components/admin/FilterInput";
import { HelpBubble } from "../components/HelpBubble";
import { MultiSelect } from "../components/MultiSelect";
import { useRoles, useTables, useDomains } from "../hooks/useAdminQueries";
import {
  useRLSRules,
  useUpsertRole,
  useDeleteRole,
  useUpsertRlsRule,
  useDeleteRlsRule,
  useRevokeRoleGrants,
} from "../hooks/useSecurityQueries";
import { RoleGrantAction } from "../components/RoleGrantAction";
import { removeUserAssignment } from "../api/admin";
import type { Role, Capability } from "../types/auth";
import type { RLSRule } from "../types/admin";
import { fetchActions } from "../api/actions";
import { useDomainFilter } from "../context/DomainFilterContext";
import { PageLoading } from "../components/PageLoading";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { ResidencyGrant } from "../components/admin/ResidencyGrant";
import { CapabilityGrid, DomainsNote } from "./security/RoleFormParts";
import { useRegionChoices } from "../hooks/useRegionQueries";
import {
  ListTable,
  ListHead,
  ListRow,
  ListExpandRow,
  ListEmpty,
  ListDetail,
  ListItems,
} from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";

/** A role may be saved once it lists a domain — its own, or one a parent role hands down. */
function listsADomain(form: { domainAccess: string[]; parentRoleId: string }): boolean {
  return form.domainAccess.length > 0 || form.parentRoleId !== "";
}

const EMPTY_ROLE = {
  id: "",
  capabilities: [] as Capability[],
  domainAccess: [] as string[],
  parentRoleId: "" as string, // REQ-1677: "" = no parent
  residencyValues: [] as string[], // REQ-1921: with data_residency, the values it covers
  // REQ-1174: per-role rate + query-complexity limits ("" = unlimited on that dimension).
  reqPerSec: "" as number | "",
  maxComplexity: "" as number | "",
  maxTimeMs: "" as number | "",
};
const EMPTY_RULE = {
  tableId: "",
  domainId: "",
  actionName: "", // REQ-1679: the rule's target when scope is "action"
  roleId: "",
  filterExpr: "",
  domainFilter: "",
  applyToDomain: false,
  applyToAction: false,
};

export function SecurityRolesPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them, and
  // offers to take the role off each grant and assignment that can be removed in place.
  const { revokeFromTable, revokeFromObject } = useRevokeRoleGrants();
  const refusal = useDependentsDialog((roleId, dependent, all, handled) => (
    <RoleGrantAction
      roleId={roleId}
      dependent={dependent}
      all={all}
      handled={handled}
      revokeFromTable={revokeFromTable}
      revokeFromObject={revokeFromObject}
      removeAssignment={removeUserAssignment}
    />
  ));
  const { t } = useTranslation();
  const { setDomains: setContextDomains, setSelectedDomain } = useDomainFilter();
  const { roles, loading: rolesLoading, refetch: refetchRoles } = useRoles();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { upsertRole } = useUpsertRole();
  const regionChoices = useRegionChoices(); // REQ-1921: no regions, no data_residency
  const { deleteRole } = useDeleteRole();
  const loading = rolesLoading || domainsLoading;
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const [showRoleForm, setShowRoleForm] = useState(false);
  const [roleForm, setRoleForm] = useState(EMPTY_ROLE);
  const [expandedRole, setExpandedRole] = useState<string | null>(null);
  const [editingRoleInRow, setEditingRoleInRow] = useState<string | null>(null);
  const [roleSearch, setRoleSearch] = useState("");

  const reload = useCallback(async () => {
    await Promise.all([refetchRoles(), refetchDomains()]);
  }, [refetchRoles, refetchDomains]);

  useEffect(() => {
    setSelectedDomain("all");
  }, [setSelectedDomain]);

  useEffect(() => {
    setContextDomains(domains.map((x) => x.id));
  }, [domains, setContextDomains]);

  const handleNewRole = () => {
    setRoleForm({ ...EMPTY_ROLE });
    setShowRoleForm(true);
    setError("");
  };

  const handleSaveRole = async () => {
    if (!roleForm.id) return;
    setSaving(true);
    setError("");
    try {
      // REQ-1174: assemble the rate_limit input from the form; omit ("" → null) unset dimensions.
      const _n = (v: number | "") => (v === "" ? null : Number(v));
      const rateLimit = {
        requestsPerSecond: _n(roleForm.reqPerSec),
        maxQueryComplexity: _n(roleForm.maxComplexity),
        maxQueryTimeMs: _n(roleForm.maxTimeMs),
      };
      const hasLimit = Object.values(rateLimit).some((v) => v !== null);
      const res = await upsertRole({
        id: roleForm.id,
        capabilities: roleForm.capabilities,
        domainAccess: roleForm.domainAccess,
        rateLimit: hasLimit ? rateLimit : null,
        parentRoleId: roleForm.parentRoleId || null, // REQ-1677
        // REQ-1921: the grant's values go with the right; without it the role lists none.
        residencyValues: roleForm.capabilities.includes("data_residency")
          ? roleForm.residencyValues
          : [],
      });
      if (!res.success) {
        setError(res.message);
        return;
      }
      setShowRoleForm(false);
      setRoleForm({ ...EMPTY_ROLE });
      setEditingRoleInRow(null);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteRole = async (id: string) => {
    setSaving(true);
    setError("");
    try {
      const result = await deleteRole(id);
      if (refusal.refused(result, id)) return;
      if (!result.success) {
        setError(result.message);
        return;
      }
      if (expandedRole === id) setExpandedRole(null);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const startEditingRole = (role: Role) => {
    setRoleForm({
      id: role.id,
      capabilities: [...role.capabilities],
      domainAccess: [...role.domain_access],
      parentRoleId: role.parentRoleId ?? "", // REQ-1677
      residencyValues: [...(role.residencyValues ?? [])], // REQ-1921
      reqPerSec: role.rateLimit?.requestsPerSecond ?? "",
      maxComplexity: role.rateLimit?.maxQueryComplexity ?? "",
      maxTimeMs: role.rateLimit?.maxQueryTimeMs ?? "",
    });
    setEditingRoleInRow(role.id);
    setError("");
  };

  const toggleCapability = (cap: Capability) => {
    setRoleForm((f) => ({
      ...f,
      capabilities: f.capabilities.includes(cap)
        ? f.capabilities.filter((c) => c !== cap)
        : [...f.capabilities, cap],
    }));
  };

  const domainOptions = [
    { id: "*", label: t("securityPage.allDomains") },
    ...domains.map((d) => ({ id: d.id, label: d.id })),
  ];

  // REQ-1940: sort and group are the shared list mechanism.
  const filteredRoles = roles.filter(
    (r) => !roleSearch.trim() || r.id.toLowerCase().includes(roleSearch.toLowerCase()),
  );
  const roleColumns: ListColumn<Role>[] = [
    { key: "id", label: t("securityPage.colId"), sortValue: (r) => r.id },
    {
      key: "capabilities",
      label: t("securityPage.colCapabilities"),
      sortValue: (r) => r.capabilities.join(", "),
      groupValue: (r) => r.capabilities.join(", "),
    },
    {
      key: "domains",
      label: t("securityPage.colDomainAccess"),
      sortValue: (r) => r.domain_access.join(", "),
      groupValue: (r) => r.domain_access.join(", ") || t("securityPage.noDomains"),
    },
  ];
  const roleSortGroup = useListSortGroup(filteredRoles, roleColumns, "roles");

  if (loading) return <PageLoading message={t("securityPage.loadingRoles")} />;

  return (
    <Stack gap="md" p="md">
      {error && (
        <Alert color="red" data-testid="security-roles-error">
          {error}
        </Alert>
      )}

      <Group justify="space-between" wrap="wrap">
        <Title order={2}>{t("securityPage.rolesHeading")}</Title>
        <FilterInput
          value={roleSearch}
          onChange={setRoleSearch}
          placeholder={t("securityPage.filterByRoleId")}
        />
        {/* Nested Group: the parent spreads its children, and the bubble explains this
            button, so it has to travel with it. */}
        <Group gap="xs">
          <Button
            data-testid="toggle-role-form"
            onClick={() => {
              if (showRoleForm) {
                setShowRoleForm(false);
              } else {
                setExpandedRole(null);
                handleNewRole();
              }
            }}
          >
            {showRoleForm ? t("securityPage.closeForm") : t("securityPage.addRole")}
          </Button>
          <HelpBubble
            title={t("securityPage.rolePurposeTitle")}
            paragraphs={[t("securityPage.rolePurposeBody"), t("securityPage.rolePurposeAdd")]}
            ariaLabel={t("securityPage.rolePurposeAria")}
            testId="roles-purpose-help"
          />
        </Group>
      </Group>

      {showRoleForm && (
        <Stack
          gap="sm"
          p="md"
          style={{ border: "1px solid var(--border)", borderRadius: "0.5rem" }}
        >
          <TextInput
            label={t("securityPage.roleId")}
            placeholder={t("securityPage.roleIdPlaceholder")}
            value={roleForm.id}
            onChange={(e) => setRoleForm({ ...roleForm, id: e.target.value })}
            data-testid="role-id-input"
          />
          <CapabilityGrid
            value={roleForm.capabilities}
            onToggle={toggleCapability}
            label={t("securityPage.capabilities")}
          />
          <ResidencyGrant
            held={roleForm.capabilities.includes("data_residency")}
            values={roleForm.residencyValues}
            regions={regionChoices.regions}
            onHeld={() => toggleCapability("data_residency")}
            onValues={(residencyValues) => setRoleForm({ ...roleForm, residencyValues })}
          />
          <MultiSelect
            label={t("securityPage.domainAccess")}
            placeholder={t("securityPage.chooseDomains")}
            options={domainOptions}
            value={roleForm.domainAccess}
            onChange={(selected) => setRoleForm({ ...roleForm, domainAccess: selected })}
          />
          {!listsADomain(roleForm) && <DomainsNote savedWithNone={false} />}
          {/* REQ-1677: single parent; the chain is walked child-first at build time. */}
          <Select
            label={t("securityPage.parentRole")}
            description={t("securityPage.parentRoleHelp")}
            placeholder={t("securityPage.parentRoleNone")}
            data={roles
              .filter((r) => r.id !== roleForm.id)
              .map((r) => ({ value: r.id, label: r.id }))}
            value={roleForm.parentRoleId || null}
            onChange={(v) => setRoleForm({ ...roleForm, parentRoleId: v ?? "" })}
            clearable
            searchable
            data-testid="role-parent-select"
          />
          {/* REQ-1174: per-role rate + query-complexity limits. Blank = unlimited on that dimension. */}
          <Text size="sm" fw={600}>
            {t("securityPage.limitsHeading", "Rate & query-complexity limits")}
          </Text>
          <Group grow>
            <NumberInput
              label={t("securityPage.rateReqPerSec", "Requests / sec")}
              placeholder={t("securityPage.unlimited", "unlimited")}
              min={1}
              data-testid="role-req-per-sec"
              value={roleForm.reqPerSec}
              onChange={(v) =>
                setRoleForm({ ...roleForm, reqPerSec: typeof v === "number" ? v : "" })
              }
            />
            <NumberInput
              label={t("securityPage.maxQueryComplexity", "Max query complexity")}
              description={t(
                "securityPage.maxQueryComplexityHint",
                "Relations, joins, columns and nested queries of one statement, on every interface. The org limit still applies.",
              )}
              placeholder={t("securityPage.unlimited", "unlimited")}
              min={1}
              data-testid="role-max-complexity"
              value={roleForm.maxComplexity}
              onChange={(v) =>
                setRoleForm({ ...roleForm, maxComplexity: typeof v === "number" ? v : "" })
              }
            />
          </Group>
          <Group grow>
            <NumberInput
              label={t("securityPage.maxQueryTimeMs", "Max query time (ms)")}
              placeholder={t("securityPage.unlimited", "unlimited")}
              min={1}
              data-testid="role-max-time-ms"
              value={roleForm.maxTimeMs}
              onChange={(v) =>
                setRoleForm({ ...roleForm, maxTimeMs: typeof v === "number" ? v : "" })
              }
            />
          </Group>
          <Group justify="flex-end">
            <Button
              variant="filled"
              color="blue"
              leftSection={<Check size={14} />}
              data-testid="save-role"
              onClick={handleSaveRole}
              disabled={saving || !listsADomain(roleForm)}
            >
              {t("securityPage.save")}
            </Button>
          </Group>
        </Stack>
      )}

      <ListTable minWidth={480} testId="roles-list">
        <ListHead
          sortGroup={roleSortGroup}
          columns={[{ col: "id" }, { col: "capabilities" }, { col: "domains" }]}
        />
        <Table.Tbody>
          <ListItems
            state={roleSortGroup}
            colSpan={3}
            rowKey={(r) => r.id}
            render={(r) => (
              <React.Fragment>
                <ListRow
                  onClick={() => {
                    setExpandedRole(expandedRole === r.id ? null : r.id);
                    setEditingRoleInRow(null);
                  }}
                >
                  <Table.Td>{r.id}</Table.Td>
                  {r.detailsHidden ? (
                    <Table.Td colSpan={2} c="dimmed" data-testid={`role-details-hidden-${r.id}`}>
                      {t("securityPage.detailsHidden")}
                    </Table.Td>
                  ) : (
                    <>
                      <Table.Td>{r.capabilities.join(", ")}</Table.Td>
                      <Table.Td data-testid={`role-domains-${r.id}`}>
                        {r.domain_access.join(", ") || t("securityPage.noDomains")}
                      </Table.Td>
                    </>
                  )}
                </ListRow>
                {expandedRole === r.id && !r.detailsHidden && (
                  <ListExpandRow colSpan={3}>
                    <ListDetail>
                      {editingRoleInRow !== r.id ? (
                        <Stack gap="xs">
                          <Text>
                            <strong>{t("securityPage.labelId")}</strong> {r.id}
                          </Text>
                          <Text>
                            <strong>{t("securityPage.labelCapabilities")}</strong>{" "}
                            {r.capabilities.join(", ") || t("securityPage.none")}
                          </Text>
                          <Text>
                            <strong>{t("securityPage.labelDomainAccess")}</strong>{" "}
                            {r.domain_access.join(", ") || t("securityPage.noDomains")}
                          </Text>
                          <Text data-testid={`role-parent-${r.id}`}>
                            <strong>{t("securityPage.labelParentRole")}</strong>{" "}
                            {r.parentRoleId || t("securityPage.none")}
                          </Text>
                          <Group gap="xs">
                            <ActionIcon
                              variant="subtle"
                              aria-label={t("securityPage.edit")}
                              data-testid={`edit-role-${r.id}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                startEditingRole(r);
                              }}
                            >
                              <Pencil size={14} />
                            </ActionIcon>
                            <ActionIcon
                              variant="subtle"
                              color="red"
                              aria-label={t("securityPage.delete")}
                              data-testid={`delete-role-${r.id}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                handleDeleteRole(r.id);
                              }}
                            >
                              <Trash2 size={14} />
                            </ActionIcon>
                          </Group>
                        </Stack>
                      ) : (
                        <Stack gap="sm">
                          <CapabilityGrid
                            value={roleForm.capabilities}
                            onToggle={toggleCapability}
                            label={t("securityPage.capabilities")}
                          />
                          <ResidencyGrant
                            held={roleForm.capabilities.includes("data_residency")}
                            values={roleForm.residencyValues}
                            regions={regionChoices.regions}
                            onHeld={() => toggleCapability("data_residency")}
                            onValues={(residencyValues) =>
                              setRoleForm({ ...roleForm, residencyValues })
                            }
                          />
                          <MultiSelect
                            label={t("securityPage.domainAccess")}
                            placeholder={t("securityPage.chooseDomains")}
                            options={domainOptions}
                            value={roleForm.domainAccess}
                            onChange={(selected) =>
                              setRoleForm({ ...roleForm, domainAccess: selected })
                            }
                          />
                          {!listsADomain(roleForm) && (
                            <DomainsNote savedWithNone={r.domain_access.length === 0} />
                          )}
                          <Select
                            label={t("securityPage.parentRole")}
                            placeholder={t("securityPage.parentRoleNone")}
                            data={roles
                              .filter((x) => x.id !== r.id)
                              .map((x) => ({ value: x.id, label: x.id }))}
                            value={roleForm.parentRoleId || null}
                            onChange={(v) => setRoleForm({ ...roleForm, parentRoleId: v ?? "" })}
                            clearable
                            searchable
                            data-testid={`role-parent-select-${r.id}`}
                          />
                          <Group justify="flex-end">
                            <Button
                              variant="default"
                              leftSection={<X size={14} />}
                              onClick={() => setEditingRoleInRow(null)}
                            >
                              {t("securityPage.cancel")}
                            </Button>
                            <Button
                              variant="filled"
                              color="blue"
                              leftSection={<Check size={14} />}
                              data-testid={`save-role-${r.id}`}
                              onClick={handleSaveRole}
                              disabled={saving || !listsADomain(roleForm)}
                            >
                              {t("securityPage.save")}
                            </Button>
                          </Group>
                        </Stack>
                      )}
                    </ListDetail>
                  </ListExpandRow>
                )}
              </React.Fragment>
            )}
          />
        </Table.Tbody>
      </ListTable>
      {refusal.dialog}
    </Stack>
  );
}

export function SecurityRlsPage() {
  const { t } = useTranslation();
  const { selectedDomain, setDomains: setContextDomains, setSelectedDomain } = useDomainFilter();
  const { roles, loading: rolesLoading } = useRoles();
  const { rlsRules: rules, loading: rulesLoading, refetch: refetchRules } = useRLSRules();
  const { tables, loading: tablesLoading, refetch: refetchTables } = useTables();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { upsertRlsRule } = useUpsertRlsRule();
  const { deleteRlsRule } = useDeleteRlsRule();
  const loading = rolesLoading || rulesLoading || tablesLoading || domainsLoading;
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const [showRuleForm, setShowRuleForm] = useState(false);
  const [ruleForm, setRuleForm] = useState(EMPTY_RULE);
  // REQ-1679: the tracked functions and webhooks a rule may target, with their domains.
  const [actions, setActions] = useState<{ name: string; domainId: string }[]>([]);
  const [expandedRule, setExpandedRule] = useState<number | null>(null);
  const [editingRuleInRow, setEditingRuleInRow] = useState<number | null>(null);
  const [ruleSearch, setRuleSearch] = useState("");
  // A table handed to the page ("rules for this table") filters the rules list, whether the page
  // was just opened or already open.
  useNavPayload<{ tableFilter?: string }>((payload) => {
    if (payload.tableFilter != null) setRuleSearch(payload.tableFilter);
  });

  const reload = useCallback(async () => {
    await Promise.all([refetchRules(), refetchTables(), refetchDomains()]);
  }, [refetchRules, refetchTables, refetchDomains]);

  useEffect(() => {
    setSelectedDomain("all");
  }, [setSelectedDomain]);

  useEffect(() => {
    setContextDomains(domains.map((x) => x.id));
  }, [domains, setContextDomains]);

  useEffect(() => {
    fetchActions()
      .then(({ functions, webhooks }) =>
        setActions([
          ...functions.map((f) => ({ name: f.name, domainId: f.domainId })),
          ...webhooks.map((w) => ({ name: w.name, domainId: w.domainId })),
        ]),
      )
      .catch((e) =>
        setError(
          t("securityPage.loadActionsFailed", {
            message: e instanceof Error ? e.message : String(e),
          }),
        ),
      );
  }, [t]);

  const normalizeDomain = (id: string) => id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const tableNameById = Object.fromEntries(tables.map((t) => [t.id, t.tableName]));
  const tableLabelById = Object.fromEntries(
    tables.map((t) => [t.id, `${normalizeDomain(t.domainId)}.${t.tableName}`]),
  );

  const handleNewRule = () => {
    setRuleForm({ ...EMPTY_RULE, domainFilter: selectedDomain !== "all" ? selectedDomain : "" });
    setShowRuleForm(true);
    setError("");
  };

  const handleSaveRule = async () => {
    const valid = ruleForm.applyToAction
      ? ruleForm.actionName && ruleForm.roleId && ruleForm.filterExpr
      : ruleForm.applyToDomain
        ? ruleForm.domainFilter && ruleForm.roleId && ruleForm.filterExpr
        : ruleForm.tableId && ruleForm.roleId && ruleForm.filterExpr;
    if (!valid) return;
    setSaving(true);
    setError("");
    try {
      const res = await upsertRlsRule({
        tableId: ruleForm.applyToDomain || ruleForm.applyToAction ? null : ruleForm.tableId || null,
        domainId: ruleForm.applyToDomain ? ruleForm.domainFilter || null : null,
        actionName: ruleForm.applyToAction ? ruleForm.actionName || null : null, // REQ-1679
        roleId: ruleForm.roleId,
        filterExpr: ruleForm.filterExpr,
      });
      if (!res.success) {
        setError(res.message);
        return;
      }
      setShowRuleForm(false);
      setRuleForm({ ...EMPTY_RULE });
      setEditingRuleInRow(null);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteRule = async (rule: RLSRule) => {
    setSaving(true);
    setError("");
    try {
      await deleteRlsRule(rule.roleId, rule.tableId, rule.domainId, rule.actionName ?? null);
      if (expandedRule === rule.id) setExpandedRule(null);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const startEditingRule = (rule: RLSRule) => {
    if (rule.actionName) {
      setRuleForm({
        ...EMPTY_RULE,
        actionName: rule.actionName,
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        domainFilter: actions.find((a) => a.name === rule.actionName)?.domainId ?? "",
        applyToAction: true,
      });
    } else if (rule.domainId) {
      setRuleForm({
        ...EMPTY_RULE,
        tableId: "",
        domainId: rule.domainId,
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        domainFilter: rule.domainId,
        applyToDomain: true,
      });
    } else {
      const tableName =
        rule.tableId != null ? (tableNameById[rule.tableId] ?? String(rule.tableId)) : "";
      const tbl = rule.tableId != null ? tables.find((t) => t.id === rule.tableId) : undefined;
      setRuleForm({
        ...EMPTY_RULE,
        tableId: tableName,
        domainId: "",
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        domainFilter: tbl ? tbl.domainId : "",
        applyToDomain: false,
      });
    }
    setEditingRuleInRow(rule.id);
    setError("");
  };

  const filtered = rules.filter((r) => {
    if (selectedDomain !== "all") {
      const ruleDomain = r.actionName
        ? actions.find((a) => a.name === r.actionName)?.domainId
        : r.domainId
          ? r.domainId
          : tables.find((t) => t.id === r.tableId)?.domainId;
      if (ruleDomain !== selectedDomain) return false;
    }
    if (!ruleSearch.trim()) return true;
    const q = ruleSearch.toLowerCase();
    const scope = r.actionName
      ? `action:${r.actionName}`
      : r.domainId
        ? `domain:${r.domainId}`
        : (tableLabelById[r.tableId!] ?? String(r.tableId));
    return r.roleId.toLowerCase().includes(q) || scope.toLowerCase().includes(q);
  });

  // REQ-1940: sort and group are the shared list mechanism.
  const ruleScope = (r: RLSRule) =>
    r.actionName
      ? r.actionName
      : r.domainId
        ? r.domainId
        : (tableLabelById[r.tableId!] ?? String(r.tableId));
  const ruleColumns: ListColumn<RLSRule>[] = [
    { key: "id", label: t("securityPage.colId"), sortValue: (r) => r.id },
    {
      key: "scope",
      label: t("securityPage.colTableOrDomain"),
      sortValue: ruleScope,
      groupValue: ruleScope,
    },
    {
      key: "role",
      label: t("securityPage.colRole"),
      sortValue: (r) => r.roleId,
      groupValue: (r) => r.roleId,
    },
    { key: "filter", label: t("securityPage.colFilter"), sortValue: (r) => r.filterExpr },
  ];
  const ruleSortGroup = useListSortGroup(filtered, ruleColumns, "rules");

  if (loading) return <PageLoading message={t("securityPage.loadingRules")} />;

  // A plain element, not a nested component: a component declared during render gets a new identity
  // every pass, so React unmounts and remounts the fields (losing focus mid-typing).
  const ruleFormFields = (
    <>
      <Group gap="sm" wrap="wrap">
        <Select
          label={t("securityPage.applyTo")}
          data={[
            { value: "table", label: t("securityPage.applyToTable") },
            { value: "domain", label: t("securityPage.applyToDomain") },
            { value: "action", label: t("securityPage.applyToAction") },
          ]}
          value={ruleForm.applyToAction ? "action" : ruleForm.applyToDomain ? "domain" : "table"}
          onChange={(v) =>
            setRuleForm({
              ...ruleForm,
              applyToDomain: v === "domain",
              applyToAction: v === "action",
              tableId: "",
              actionName: "",
            })
          }
          allowDeselect={false}
          data-testid="rule-apply-to"
        />
        <Select
          label={t("securityPage.domain")}
          placeholder={t("securityPage.selectPlaceholder")}
          data={domains.map((d) => ({ value: d.id, label: d.id }))}
          value={ruleForm.domainFilter || null}
          onChange={(v) => setRuleForm({ ...ruleForm, domainFilter: v ?? "", tableId: "" })}
        />
        {ruleForm.applyToAction && (
          <Select
            label={t("securityPage.action")}
            placeholder={t("securityPage.selectPlaceholder")}
            data={actions
              .filter((a) => !ruleForm.domainFilter || a.domainId === ruleForm.domainFilter)
              .map((a) => ({ value: a.name, label: a.name }))}
            value={ruleForm.actionName || null}
            onChange={(v) => setRuleForm({ ...ruleForm, actionName: v ?? "" })}
            searchable
            data-testid="rule-action-select"
          />
        )}
        {!ruleForm.applyToDomain && !ruleForm.applyToAction && (
          <Select
            label={t("securityPage.table")}
            placeholder={t("securityPage.selectPlaceholder")}
            data={tables
              .filter((tb) => !ruleForm.domainFilter || tb.domainId === ruleForm.domainFilter)
              .map((tb) => ({ value: tb.tableName, label: tb.tableName }))}
            value={ruleForm.tableId || null}
            onChange={(v) => setRuleForm({ ...ruleForm, tableId: v ?? "" })}
          />
        )}
        <Select
          label={t("securityPage.role")}
          placeholder={t("securityPage.selectPlaceholder")}
          data={roles.map((r) => ({ value: r.id, label: r.id }))}
          value={ruleForm.roleId || null}
          onChange={(v) => setRuleForm({ ...ruleForm, roleId: v ?? "" })}
          data-testid="rule-role-select"
        />
      </Group>
      <Textarea
        label={t("securityPage.filterExpression")}
        placeholder={t("securityPage.filterExpressionPlaceholder")}
        rows={2}
        value={ruleForm.filterExpr}
        onChange={(e) => setRuleForm({ ...ruleForm, filterExpr: e.target.value })}
        styles={{ input: { fontFamily: "monospace", fontSize: "0.875rem" } }}
      />
    </>
  );

  return (
    <Stack gap="md" p="md">
      {error && (
        <Alert color="red" data-testid="security-rls-error">
          {error}
        </Alert>
      )}

      <Group justify="space-between" wrap="wrap">
        <Title order={2}>{t("securityPage.rlsHeading")}</Title>
        <FilterInput
          value={ruleSearch}
          onChange={setRuleSearch}
          placeholder={t("securityPage.filterByRoleOrTable")}
        />
        {/* Nested Group: the parent spreads its children, and the bubble explains this
            button, so it has to travel with it. */}
        <Group gap="xs">
          <Button
            data-testid="toggle-rule-form"
            onClick={() => {
              if (showRuleForm) {
                setShowRuleForm(false);
              } else {
                setExpandedRule(null);
                handleNewRule();
              }
            }}
          >
            {showRuleForm ? t("securityPage.closeForm") : t("securityPage.addRls")}
          </Button>
          <HelpBubble
            title={t("securityPage.purposeTitle")}
            paragraphs={[t("securityPage.purposeBody"), t("securityPage.purposeAdd")]}
            ariaLabel={t("securityPage.purposeAria")}
            testId="security-purpose-help"
          />
        </Group>
      </Group>

      {showRuleForm && (
        <Stack
          gap="sm"
          p="md"
          style={{ border: "1px solid var(--border)", borderRadius: "0.5rem" }}
        >
          {ruleFormFields}
          <Group justify="flex-end">
            <Button
              variant="filled"
              color="blue"
              leftSection={<Check size={14} />}
              data-testid="save-rule"
              onClick={handleSaveRule}
              disabled={saving}
            >
              {t("securityPage.save")}
            </Button>
          </Group>
        </Stack>
      )}

      <ListTable minWidth={640} testId="rules-list">
        <ListHead
          sortGroup={ruleSortGroup}
          columns={[{ col: "id" }, { col: "scope" }, { col: "role" }, { col: "filter" }]}
        />
        <Table.Tbody>
          {filtered.length === 0 && (
            <ListEmpty colSpan={4}>
              {rules.length === 0
                ? t("securityPage.noRulesDefined")
                : t("securityPage.noRulesMatchFilter")}
            </ListEmpty>
          )}
          <ListItems
            state={ruleSortGroup}
            colSpan={4}
            rowKey={(r) => r.id}
            render={(r) => (
              <React.Fragment>
                <ListRow
                  onClick={() => {
                    setExpandedRule(expandedRule === r.id ? null : r.id);
                    setEditingRuleInRow(null);
                  }}
                >
                  <Table.Td>{r.id}</Table.Td>
                  <Table.Td>
                    {r.actionName ? (
                      <span data-testid={`rule-scope-${r.id}`}>
                        <Text span c="dimmed" fz="0.75em">
                          {t("securityPage.actionPrefix")}{" "}
                        </Text>
                        {r.actionName}
                      </span>
                    ) : r.domainId ? (
                      <>
                        <Text span c="dimmed" fz="0.75em">
                          {t("securityPage.domainPrefix")}{" "}
                        </Text>
                        {r.domainId}
                      </>
                    ) : (
                      (tableLabelById[r.tableId!] ?? String(r.tableId))
                    )}
                  </Table.Td>
                  <Table.Td>{r.roleId}</Table.Td>
                  <Table.Td>
                    <Text component="code">{r.filterExpr}</Text>
                  </Table.Td>
                </ListRow>
                {expandedRule === r.id && (
                  <ListExpandRow colSpan={4}>
                    <ListDetail>
                      {editingRuleInRow !== r.id ? (
                        <Stack gap="xs">
                          <Text>
                            <strong>{t("securityPage.labelId")}</strong> {r.id}
                          </Text>
                          {r.actionName ? (
                            <Text>
                              <strong>{t("securityPage.labelAction")}</strong> {r.actionName}
                            </Text>
                          ) : r.domainId ? (
                            <Text>
                              <strong>{t("securityPage.labelDomain")}</strong> {r.domainId}
                            </Text>
                          ) : (
                            <Text>
                              <strong>{t("securityPage.labelTable")}</strong>{" "}
                              {tableLabelById[r.tableId!] ?? String(r.tableId)}
                            </Text>
                          )}
                          <Text>
                            <strong>{t("securityPage.labelRole")}</strong> {r.roleId}
                          </Text>
                          <Text>
                            <strong>{t("securityPage.labelFilter")}</strong>{" "}
                            <Text component="code" span>
                              {r.filterExpr}
                            </Text>
                          </Text>
                          <Group gap="xs">
                            <ActionIcon
                              variant="subtle"
                              aria-label={t("securityPage.edit")}
                              data-testid={`edit-rule-${r.id}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                startEditingRule(r);
                              }}
                            >
                              <Pencil size={14} />
                            </ActionIcon>
                            <ActionIcon
                              variant="subtle"
                              color="red"
                              aria-label={t("securityPage.delete")}
                              data-testid={`delete-rule-${r.id}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                handleDeleteRule(r);
                              }}
                            >
                              <Trash2 size={14} />
                            </ActionIcon>
                          </Group>
                        </Stack>
                      ) : (
                        <Stack gap="sm">
                          {ruleFormFields}
                          <Group justify="flex-end">
                            <Button
                              variant="default"
                              leftSection={<X size={14} />}
                              onClick={() => setEditingRuleInRow(null)}
                            >
                              {t("securityPage.cancel")}
                            </Button>
                            <Button
                              variant="filled"
                              color="blue"
                              leftSection={<Check size={14} />}
                              onClick={handleSaveRule}
                              disabled={saving}
                            >
                              {t("securityPage.save")}
                            </Button>
                          </Group>
                        </Stack>
                      )}
                    </ListDetail>
                  </ListExpandRow>
                )}
              </React.Fragment>
            )}
          />
        </Table.Tbody>
      </ListTable>
    </Stack>
  );
}

export { SecurityRolesPage as SecurityPage };
