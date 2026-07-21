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
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Trash2, Pencil, Check, X } from "lucide-react";
import {
  Alert,
  Button,
  Checkbox,
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
import { MultiSelect } from "../components/MultiSelect";
import {
  useRoles,
  useRLSRules,
  useTables,
  useDomains,
  useUpsertRole,
  useDeleteRole,
  useUpsertRlsRule,
  useDeleteRlsRule,
} from "../hooks/useAdminQueries";
import type { Role, Capability } from "../types/auth";
import type { RLSRule } from "../types/admin";
import { useDomainFilter } from "../context/DomainFilterContext";

const ALL_CAPABILITIES: Capability[] = [
  "source_registration",
  "table_registration",
  "create_relationship",
  "access_config",
  "query_development",
  "approve_view",
  "full_results",
  "admin",
  "usage",
  "read_restricted",
  "approve_relationship",
  "create_view",
  "column_grant",
  "user_management",
  "masking_config",
  "superadmin",
];

const EMPTY_ROLE = {
  id: "",
  capabilities: [] as Capability[],
  domainAccess: [] as string[],
  // REQ-1174: per-role rate + query-complexity limits ("" = unlimited on that dimension).
  reqPerSec: "" as number | "",
  maxDepth: "" as number | "",
  maxNodes: "" as number | "",
  maxTimeMs: "" as number | "",
};
const EMPTY_RULE = {
  tableId: "",
  domainId: "",
  roleId: "",
  filterExpr: "",
  domainFilter: "",
  applyToDomain: false,
};

function CapabilityGrid({
  value,
  onToggle,
  label,
}: {
  value: Capability[];
  onToggle: (cap: Capability) => void;
  label: string;
}) {
  return (
    <Checkbox.Group label={label} value={value} data-testid="capability-grid">
      <Group gap="sm" mt="xs" style={{ rowGap: "0.35rem" }}>
        {ALL_CAPABILITIES.map((cap) => (
          <Checkbox
            key={cap}
            label={cap}
            checked={value.includes(cap)}
            onChange={() => onToggle(cap)}
            size="sm"
          />
        ))}
      </Group>
    </Checkbox.Group>
  );
}

export function SecurityRolesPage() {
  const { t } = useTranslation();
  const { setDomains: setContextDomains, setSelectedDomain } = useDomainFilter();
  const { roles, loading: rolesLoading, refetch: refetchRoles } = useRoles();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { upsertRole } = useUpsertRole();
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
        maxQueryDepth: _n(roleForm.maxDepth),
        maxQueryNodes: _n(roleForm.maxNodes),
        maxQueryTimeMs: _n(roleForm.maxTimeMs),
      };
      const hasLimit = Object.values(rateLimit).some((v) => v !== null);
      const res = await upsertRole({
        id: roleForm.id,
        capabilities: roleForm.capabilities,
        domainAccess: roleForm.domainAccess,
        rateLimit: hasLimit ? rateLimit : null,
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
      await deleteRole(id);
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
      reqPerSec: role.rateLimit?.requestsPerSecond ?? "",
      maxDepth: role.rateLimit?.maxQueryDepth ?? "",
      maxNodes: role.rateLimit?.maxQueryNodes ?? "",
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

  if (loading) return <Text p="md">{t("securityPage.loadingRoles")}</Text>;

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
      </Group>

      {showRoleForm && (
        <Stack gap="sm" p="md" style={{ border: "1px solid var(--border)", borderRadius: "0.5rem" }}>
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
          <MultiSelect
            label={t("securityPage.domainAccess")}
            options={domainOptions}
            value={roleForm.domainAccess}
            onChange={(selected) => setRoleForm({ ...roleForm, domainAccess: selected })}
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
              label={t("securityPage.maxQueryDepth", "Max query depth")}
              placeholder={t("securityPage.unlimited", "unlimited")}
              min={1}
              data-testid="role-max-depth"
              value={roleForm.maxDepth}
              onChange={(v) =>
                setRoleForm({ ...roleForm, maxDepth: typeof v === "number" ? v : "" })
              }
            />
          </Group>
          <Group grow>
            <NumberInput
              label={t("securityPage.maxQueryNodes", "Max query nodes")}
              placeholder={t("securityPage.unlimited", "unlimited")}
              min={1}
              data-testid="role-max-nodes"
              value={roleForm.maxNodes}
              onChange={(v) =>
                setRoleForm({ ...roleForm, maxNodes: typeof v === "number" ? v : "" })
              }
            />
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
            <ActionIcon
              variant="filled"
              color="blue"
              aria-label={t("securityPage.save")}
              data-testid="save-role"
              onClick={handleSaveRole}
              disabled={saving}
            >
              <Check size={14} />
            </ActionIcon>
          </Group>
        </Stack>
      )}

      <Table.ScrollContainer minWidth={480}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("securityPage.colId")}</Table.Th>
              <Table.Th>{t("securityPage.colCapabilities")}</Table.Th>
              <Table.Th>{t("securityPage.colDomainAccess")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {roles
              .filter(
                (r) => !roleSearch.trim() || r.id.toLowerCase().includes(roleSearch.toLowerCase()),
              )
              .map((r) => (
                <React.Fragment key={r.id}>
                  <Table.Tr
                    style={{
                      cursor: "pointer",
                      background: expandedRole === r.id ? "var(--surface)" : undefined,
                    }}
                    onClick={() => {
                      setExpandedRole(expandedRole === r.id ? null : r.id);
                      setEditingRoleInRow(null);
                    }}
                  >
                    <Table.Td>{r.id}</Table.Td>
                    <Table.Td>{r.capabilities.join(", ")}</Table.Td>
                    <Table.Td>{r.domain_access.join(", ")}</Table.Td>
                  </Table.Tr>
                  {expandedRole === r.id && (
                    <Table.Tr>
                      <Table.Td colSpan={3} style={{ background: "var(--bg)" }}>
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
                              {r.domain_access.join(", ") || t("securityPage.none")}
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
                            <MultiSelect
                              label={t("securityPage.domainAccess")}
                              options={domainOptions}
                              value={roleForm.domainAccess}
                              onChange={(selected) =>
                                setRoleForm({ ...roleForm, domainAccess: selected })
                              }
                            />
                            <Group justify="flex-end">
                              <ActionIcon
                                variant="subtle"
                                aria-label={t("securityPage.cancel")}
                                onClick={() => setEditingRoleInRow(null)}
                              >
                                <X size={14} />
                              </ActionIcon>
                              <ActionIcon
                                variant="filled"
                                color="blue"
                                aria-label={t("securityPage.save")}
                                onClick={handleSaveRole}
                                disabled={saving}
                              >
                                <Check size={14} />
                              </ActionIcon>
                            </Group>
                          </Stack>
                        )}
                      </Table.Td>
                    </Table.Tr>
                  )}
                </React.Fragment>
              ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}

export function SecurityRlsPage() {
  const { t } = useTranslation();
  const location = useLocation();
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
  const [expandedRule, setExpandedRule] = useState<number | null>(null);
  const [editingRuleInRow, setEditingRuleInRow] = useState<number | null>(null);
  const [ruleSearch, setRuleSearch] = useState(
    (location.state as { tableFilter?: string } | null)?.tableFilter ?? "",
  );

  const reload = useCallback(async () => {
    await Promise.all([refetchRules(), refetchTables(), refetchDomains()]);
  }, [refetchRules, refetchTables, refetchDomains]);

  useEffect(() => {
    setSelectedDomain("all");
  }, [setSelectedDomain]);

  useEffect(() => {
    setContextDomains(domains.map((x) => x.id));
  }, [domains, setContextDomains]);

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
    const valid = ruleForm.applyToDomain
      ? ruleForm.domainFilter && ruleForm.roleId && ruleForm.filterExpr
      : ruleForm.tableId && ruleForm.roleId && ruleForm.filterExpr;
    if (!valid) return;
    setSaving(true);
    setError("");
    try {
      const res = await upsertRlsRule({
        tableId: ruleForm.applyToDomain ? null : ruleForm.tableId || null,
        domainId: ruleForm.applyToDomain ? ruleForm.domainFilter || null : null,
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
      await deleteRlsRule(rule.roleId, rule.tableId, rule.domainId);
      if (expandedRule === rule.id) setExpandedRule(null);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const startEditingRule = (rule: RLSRule) => {
    if (rule.domainId) {
      setRuleForm({
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

  if (loading) return <Text p="md">{t("securityPage.loadingRules")}</Text>;

  const RuleFormFields = () => (
    <>
      <Group gap="sm" wrap="wrap">
        <Select
          label={t("securityPage.applyTo")}
          data={[
            { value: "table", label: t("securityPage.applyToTable") },
            { value: "domain", label: t("securityPage.applyToDomain") },
          ]}
          value={ruleForm.applyToDomain ? "domain" : "table"}
          onChange={(v) =>
            setRuleForm({
              ...ruleForm,
              applyToDomain: v === "domain",
              tableId: "",
            })
          }
          allowDeselect={false}
        />
        <Select
          label={t("securityPage.domain")}
          placeholder={t("securityPage.selectPlaceholder")}
          data={domains.map((d) => ({ value: d.id, label: d.id }))}
          value={ruleForm.domainFilter || null}
          onChange={(v) => setRuleForm({ ...ruleForm, domainFilter: v ?? "", tableId: "" })}
        />
        {!ruleForm.applyToDomain && (
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

  const filtered = rules.filter((r) => {
    if (selectedDomain !== "all") {
      const ruleDomain = r.domainId
        ? r.domainId
        : tables.find((t) => t.id === r.tableId)?.domainId;
      if (ruleDomain !== selectedDomain) return false;
    }
    if (!ruleSearch.trim()) return true;
    const q = ruleSearch.toLowerCase();
    const scope = r.domainId
      ? `domain:${r.domainId}`
      : (tableLabelById[r.tableId!] ?? String(r.tableId));
    return r.roleId.toLowerCase().includes(q) || scope.toLowerCase().includes(q);
  });

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
      </Group>

      {showRuleForm && (
        <Stack gap="sm" p="md" style={{ border: "1px solid var(--border)", borderRadius: "0.5rem" }}>
          <RuleFormFields />
          <Group justify="flex-end">
            <ActionIcon
              variant="filled"
              color="blue"
              aria-label={t("securityPage.save")}
              data-testid="save-rule"
              onClick={handleSaveRule}
              disabled={saving}
            >
              <Check size={14} />
            </ActionIcon>
          </Group>
        </Stack>
      )}

      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("securityPage.colId")}</Table.Th>
              <Table.Th>{t("securityPage.colTableOrDomain")}</Table.Th>
              <Table.Th>{t("securityPage.colRole")}</Table.Th>
              <Table.Th>{t("securityPage.colFilter")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filtered.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={4} ta="center" c="dimmed">
                  {rules.length === 0
                    ? t("securityPage.noRulesDefined")
                    : t("securityPage.noRulesMatchFilter")}
                </Table.Td>
              </Table.Tr>
            )}
            {filtered.map((r) => (
              <React.Fragment key={r.id}>
                <Table.Tr
                  style={{
                    cursor: "pointer",
                    background: expandedRule === r.id ? "var(--surface)" : undefined,
                  }}
                  onClick={() => {
                    setExpandedRule(expandedRule === r.id ? null : r.id);
                    setEditingRuleInRow(null);
                  }}
                >
                  <Table.Td>{r.id}</Table.Td>
                  <Table.Td>
                    {r.domainId ? (
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
                </Table.Tr>
                {expandedRule === r.id && (
                  <Table.Tr>
                    <Table.Td colSpan={4} style={{ background: "var(--bg)" }}>
                      {editingRuleInRow !== r.id ? (
                        <Stack gap="xs">
                          <Text>
                            <strong>{t("securityPage.labelId")}</strong> {r.id}
                          </Text>
                          {r.domainId ? (
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
                          <RuleFormFields />
                          <Group justify="flex-end">
                            <ActionIcon
                              variant="subtle"
                              aria-label={t("securityPage.cancel")}
                              onClick={() => setEditingRuleInRow(null)}
                            >
                              <X size={14} />
                            </ActionIcon>
                            <ActionIcon
                              variant="filled"
                              color="blue"
                              aria-label={t("securityPage.save")}
                              onClick={handleSaveRule}
                              disabled={saving}
                            >
                              <Check size={14} />
                            </ActionIcon>
                          </Group>
                        </Stack>
                      )}
                    </Table.Td>
                  </Table.Tr>
                )}
              </React.Fragment>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}

export { SecurityRolesPage as SecurityPage };
