// Copyright (c) 2026 Kenneth Stott
// Canary: 49e6df06-afa4-493c-9489-f83cf19cb3b1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The RLS page of the Security area: the list of row-level rules, and a rule's detail or form in
// the right-hand panel.

import { Tip } from "../../components/Tip";
import { RlsRuleRead } from "./RlsRuleRead";
import React, { useState, useEffect, useCallback } from "react";
import { useNavPayload } from "../../hooks/useNavPayload";
import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { Trash2, Pencil } from "lucide-react";
import {
  Alert,
  Button,
  Group,
  ActionIcon,
  Select,
  Stack,
  Table,
  Text,
  Textarea,
  Title,
} from "@mantine/core";
import { FilterInput } from "../../components/admin/FilterInput";
import { HelpBubble } from "../../components/HelpBubble";
import { useRoles, useTables, useDomains } from "../../hooks/useAdminQueries";
import { useRLSRules, useUpsertRlsRule, useDeleteRlsRule } from "../../hooks/useSecurityQueries";
import { baseTagId, type RLSRule } from "../../types/admin";
import { useTags, useTagAssignments } from "../../hooks/useTagQueries";
import { fetchActions } from "../../api/actions";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { PageLoading } from "../../components/PageLoading";
import {
  ListTable,
  ListHead,
  ListRow,
  ListEmpty,
  ListItems,
} from "../../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../../components/list/useListSortGroup";
import { PanelActions, SplitPane } from "../../components/list/SplitPane";
import { relationName } from "../../naming";
import { serverMessage } from "../../i18n/serverMessage";
import { FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";

const EMPTY_RULE = {
  tableId: "",
  domainId: "",
  actionName: "", // REQ-1679: the rule's target when scope is "action"
  tagId: "", // REQ-1384: the rule's target when scope is "tag"
  roleId: "",
  filterExpr: "",
  description: "", // REQ-2081: required
  domainFilter: "",
  applyToDomain: false,
  applyToAction: false,
  applyToTag: false,
};

export function SecurityRlsPage() {
  const { t } = useTranslation();
  const {
    domains: filterDomains,
    checkedDomains,
    setDomains: setContextDomains,
    setSelectedDomain,
  } = useDomainFilter();
  // The navbar's multi-select; none or every domain checked is no narrowing (as on Tables).
  const narrowTo =
    checkedDomains.size > 0 && checkedDomains.size < filterDomains.length ? checkedDomains : null;
  const { roles, loading: rolesLoading } = useRoles();
  const { rlsRules: rules, loading: rulesLoading, refetch: refetchRules } = useRLSRules();
  const { tables, loading: tablesLoading, refetch: refetchTables } = useTables();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { upsertRlsRule } = useUpsertRlsRule();
  const { deleteRlsRule } = useDeleteRlsRule();
  // REQ-1384: a rule may target a tag. Only a user tag that applies to tables takes one.
  const { tags } = useTags();
  const { tagAssignments } = useTagAssignments();
  const filterableTags = tags.filter(
    (tg) => !tg.isSystem && !tg.derived && tg.appliesTo.includes("table"),
  );
  // The tables a tag is on, whatever value it was assigned with ("entity:customer" is `entity`).
  const tablesCarrying = (tagId: string) =>
    tagAssignments
      .filter((a) => a.objectType === "table" && baseTagId(a.tagId) === tagId)
      .map((a) => a.tableId);
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

  const tableNameById = Object.fromEntries(tables.map((t) => [t.id, t.tableName]));
  const tableLabelById = Object.fromEntries(tables.map((t) => [t.id, relationName(t)]));

  const handleNewRule = () => {
    setRuleForm({
      ...EMPTY_RULE,
      domainFilter: narrowTo && narrowTo.size === 1 ? [...narrowTo][0] : "",
    });
    setShowRuleForm(true);
    setError("");
  };

  const handleSaveRule = async () => {
    // The footer names every field a rule still lacks, by its label, instead of a Save that
    // does nothing.
    const target = ruleForm.applyToTag
      ? [ruleForm.tagId, "securityPage.tag"]
      : ruleForm.applyToAction
        ? [ruleForm.actionName, "securityPage.action"]
        : ruleForm.applyToDomain
          ? [ruleForm.domainFilter, "securityPage.domain"]
          : [ruleForm.tableId, "securityPage.table"];
    const missing = [
      target,
      [ruleForm.roleId, "securityPage.role"],
      [ruleForm.filterExpr, "securityPage.filterExpression"],
      [ruleForm.description.trim(), "securityPage.ruleDescription"],
    ]
      .filter(([value]) => !value)
      .map(([, label]) => t(label));
    if (missing.length > 0) {
      setError(t("securityPage.ruleMissing", { fields: missing.join(", ") }));
      return;
    }
    setSaving(true);
    setError("");
    try {
      const res = await upsertRlsRule({
        tableId:
          ruleForm.applyToDomain || ruleForm.applyToAction || ruleForm.applyToTag
            ? null
            : ruleForm.tableId || null,
        domainId: ruleForm.applyToDomain ? ruleForm.domainFilter || null : null,
        actionName: ruleForm.applyToAction ? ruleForm.actionName || null : null, // REQ-1679
        tagId: ruleForm.applyToTag ? ruleForm.tagId || null : null, // REQ-1384
        roleId: ruleForm.roleId,
        filterExpr: ruleForm.filterExpr,
        description: ruleForm.description.trim(),
      });
      if (!res.success) {
        setError(serverMessage(res, res.message));
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
      await deleteRlsRule(
        rule.roleId,
        rule.tableId,
        rule.domainId,
        rule.actionName ?? null,
        rule.tagId ?? null,
      );
      if (expandedRule === rule.id) setExpandedRule(null);
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const startEditingRule = (rule: RLSRule) => {
    if (rule.tagId) {
      setRuleForm({
        ...EMPTY_RULE,
        tagId: rule.tagId,
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        description: rule.description,
        applyToTag: true,
      });
    } else if (rule.actionName) {
      setRuleForm({
        ...EMPTY_RULE,
        actionName: rule.actionName,
        roleId: rule.roleId,
        filterExpr: rule.filterExpr,
        description: rule.description,
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
        description: rule.description,
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
        description: rule.description,
        domainFilter: tbl ? tbl.domainId : "",
        applyToDomain: false,
      });
    }
    setEditingRuleInRow(rule.id);
    setError("");
  };

  const filtered = rules.filter((r) => {
    if (narrowTo && r.tagId) {
      // A tag rule belongs to every domain that holds a table carrying the tag.
      const carrying = tablesCarrying(r.tagId);
      if (!tables.some((tb) => carrying.includes(tb.id) && narrowTo.has(tb.domainId))) return false;
    } else if (narrowTo) {
      const ruleDomain = r.actionName
        ? actions.find((a) => a.name === r.actionName)?.domainId
        : r.domainId
          ? r.domainId
          : tables.find((t) => t.id === r.tableId)?.domainId;
      if (ruleDomain && !narrowTo.has(ruleDomain)) return false;
    }
    if (!ruleSearch.trim()) return true;
    const q = ruleSearch.toLowerCase();
    const scope = r.tagId
      ? `tag:${r.tagId}`
      : r.actionName
        ? `action:${r.actionName}`
        : r.domainId
          ? `domain:${r.domainId}`
          : (tableLabelById[r.tableId!] ?? String(r.tableId));
    return (
      r.roleId.toLowerCase().includes(q) ||
      scope.toLowerCase().includes(q) ||
      r.description.toLowerCase().includes(q)
    );
  });

  // REQ-1940: sort and group are the shared list mechanism.
  const ruleScope = (r: RLSRule) =>
    r.tagId
      ? r.tagId
      : r.actionName
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
    // REQ-2081: what each rule is for, beside what it filters.
    {
      key: "description",
      label: t("securityPage.colDescription"),
      sortValue: (r) => r.description,
    },
  ];
  const ruleSortGroup = useListSortGroup(filtered, ruleColumns, "rules");

  if (loading) return <PageLoading message={t("securityPage.loadingRules")} />;

  // A plain element, not a nested component: a component declared during render gets a new identity
  // every pass, so React unmounts and remounts the fields (losing focus mid-typing).
  const ruleFormFields = (
    <>
      <Group gap="sm" wrap="wrap">
        <FormLayout>
          <FormField size="medium">
            <Select
              label={t("securityPage.applyTo")}
              data={[
                { value: "table", label: t("securityPage.applyToTable") },
                { value: "domain", label: t("securityPage.applyToDomain") },
                { value: "action", label: t("securityPage.applyToAction") },
                { value: "tag", label: t("securityPage.applyToTag") },
              ]}
              value={
                ruleForm.applyToTag
                  ? "tag"
                  : ruleForm.applyToAction
                    ? "action"
                    : ruleForm.applyToDomain
                      ? "domain"
                      : "table"
              }
              onChange={(v) =>
                setRuleForm({
                  ...ruleForm,
                  applyToDomain: v === "domain",
                  applyToAction: v === "action",
                  applyToTag: v === "tag",
                  tableId: "",
                  actionName: "",
                  tagId: "",
                })
              }
              allowDeselect={false}
              data-testid="rule-apply-to"
            />
          </FormField>
          {/* A tag reaches across domains, so a tag rule has no domain to pick. */}
          {!ruleForm.applyToTag && (
            <FormField size="medium">
              <Select
                label={t("securityPage.domain")}
                placeholder={t("securityPage.selectPlaceholder")}
                data={domains.map((d) => ({ value: d.id, label: d.id }))}
                value={ruleForm.domainFilter || null}
                onChange={(v) => setRuleForm({ ...ruleForm, domainFilter: v ?? "", tableId: "" })}
              />
            </FormField>
          )}
          {ruleForm.applyToTag && (
            <FormField
              size="medium"
              hint={
                ruleForm.tagId
                  ? t("securityPage.tagTableCount", {
                      count: tablesCarrying(ruleForm.tagId).length,
                    })
                  : undefined
              }
            >
              <Select
                label={t("securityPage.tag")}
                placeholder={t("securityPage.selectPlaceholder")}
                data={filterableTags.map((tg) => ({ value: tg.id, label: tg.id }))}
                value={ruleForm.tagId || null}
                onChange={(v) => setRuleForm({ ...ruleForm, tagId: v ?? "" })}
                searchable
                data-testid="rule-tag-select"
              />
            </FormField>
          )}
          {ruleForm.applyToAction && (
            <FormField size="medium">
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
            </FormField>
          )}
          {!ruleForm.applyToDomain && !ruleForm.applyToAction && !ruleForm.applyToTag && (
            <FormField size="medium">
              <Select
                label={t("securityPage.table")}
                placeholder={t("securityPage.selectPlaceholder")}
                data={tables
                  .filter((tb) => !ruleForm.domainFilter || tb.domainId === ruleForm.domainFilter)
                  .map((tb) => ({ value: tb.tableName, label: tb.tableName }))}
                value={ruleForm.tableId || null}
                onChange={(v) => setRuleForm({ ...ruleForm, tableId: v ?? "" })}
              />
            </FormField>
          )}
          <FormField size="medium">
            <Select
              label={t("securityPage.role")}
              placeholder={t("securityPage.selectPlaceholder")}
              data={roles.map((r) => ({ value: r.id, label: r.id }))}
              value={ruleForm.roleId || null}
              onChange={(v) => setRuleForm({ ...ruleForm, roleId: v ?? "" })}
              data-testid="rule-role-select"
            />
          </FormField>
        </FormLayout>
      </Group>
      <FormLayout>
        <FormField size="full">
          <Textarea
            label={t("securityPage.filterExpression")}
            placeholder={t("securityPage.filterExpressionPlaceholder")}
            rows={2}
            value={ruleForm.filterExpr}
            onChange={(e) => setRuleForm({ ...ruleForm, filterExpr: e.target.value })}
            styles={{ input: { fontFamily: "monospace", fontSize: "0.875rem" } }}
          />
        </FormField>
        {/* REQ-2081: required; an access review reads it beside the filter. */}
        <FormField size="full" help={t("securityPage.ruleDescriptionHelp")}>
          <Textarea
            label={t("securityPage.ruleDescription")}
            placeholder={t("securityPage.ruleDescriptionPlaceholder")}
            required
            autosize
            minRows={2}
            value={ruleForm.description}
            onChange={(e) => setRuleForm({ ...ruleForm, description: e.target.value })}
            data-testid="rule-description"
          />
        </FormField>
      </FormLayout>
    </>
  );

  // The right-hand panel: the form that adds a rule while it is open, otherwise the selected
  // rule -- its detail, or the form that edits it.
  const closeRulePanel = () => {
    setShowRuleForm(false);
    setEditingRuleInRow(null);
    setExpandedRule(null);
  };
  const selectedRule =
    expandedRule === null ? null : (rules.find((x) => x.id === expandedRule) ?? null);
  const rulePanelTitle = showRuleForm
    ? t("securityPage.panelNewRule")
    : selectedRule
      ? t("securityPage.panelRule", { id: selectedRule.id })
      : "";
  const rulePanelBody = showRuleForm ? (
    <Stack gap="sm">
      {ruleFormFields}
      <FormFooter
        panel
        problem={error || undefined}
        onSave={handleSaveRule}
        saving={saving}
        onCancel={() => setShowRuleForm(false)}
        saveProps={{ "data-testid": "save-rule" }}
      />
    </Stack>
  ) : selectedRule ? (
    ((r) => (
      <>
        {/* Its actions sit in the panel's head; while its form is open the head is the form's. */}
        {editingRuleInRow !== r.id && (
          <PanelActions>
            <fieldset className="panel-action-set">
              <Group gap="xs">
                <Tip text={t("securityPage.edit")} named>
                  <ActionIcon
                    aria-label={t("securityPage.edit")}
                    data-testid={`edit-rule-${r.id}`}
                    onClick={(e) => {
                      e.stopPropagation();
                      startEditingRule(r);
                    }}
                  >
                    <Pencil size={14} />
                  </ActionIcon>
                </Tip>
                <ConfirmDialog
                  title={t("securityPage.deleteRuleTitle")}
                  consequence={t("securityPage.deleteRuleConsequence", {
                    role: r.roleId,
                  })}
                  onConfirm={() => handleDeleteRule(r)}
                >
                  {(open) => (
                    <Tip text={t("securityPage.delete")} named>
                      <ActionIcon
                        color="red"
                        aria-label={t("securityPage.delete")}
                        data-testid={`delete-rule-${r.id}`}
                        onClick={(e) => {
                          e.stopPropagation();
                          open();
                        }}
                      >
                        <Trash2 size={14} />
                      </ActionIcon>
                    </Tip>
                  )}
                </ConfirmDialog>
              </Group>
            </fieldset>
          </PanelActions>
        )}
        {editingRuleInRow !== r.id ? (
          <RlsRuleRead
            rule={r}
            domain={
              r.tagId
                ? null
                : r.actionName
                  ? (actions.find((a) => a.name === r.actionName)?.domainId ?? null)
                  : r.domainId
                    ? r.domainId
                    : (tables.find((tb) => tb.id === r.tableId)?.domainId ?? null)
            }
            tableLabel={r.tableId != null ? (tableLabelById[r.tableId] ?? String(r.tableId)) : null}
            tagTables={r.tagId ? tablesCarrying(r.tagId).length : null}
          />
        ) : (
          <Stack gap="sm">
            {ruleFormFields}
            <FormFooter
              panel
              problem={error || undefined}
              onSave={handleSaveRule}
              saving={saving}
              onCancel={() => setEditingRuleInRow(null)}
            />
          </Stack>
        )}
      </>
    ))(selectedRule)
  ) : null;

  return (
    // The list and its panel share the height the heading leaves; each scrolls by itself.
    <Stack gap="md" p="md" className="page-sticky-head">
      {/* A form that did not save says so under the panel's head; this is for everything else. */}
      {error && !showRuleForm && editingRuleInRow === null && (
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
          {!showRuleForm && (
            <Button
              data-testid="toggle-rule-form"
              onClick={() => {
                setExpandedRule(null);
                handleNewRule();
              }}
            >
              {t("securityPage.addRls")}
            </Button>
          )}
          <HelpBubble
            title={t("securityPage.purposeTitle")}
            paragraphs={[t("securityPage.purposeBody"), t("securityPage.purposeAdd")]}
            ariaLabel={t("securityPage.purposeAria")}
            testId="security-purpose-help"
          />
        </Group>
      </Group>

      <SplitPane
        testId="rules-split"
        storageKey="provisa.rls.detailWidth"
        title={rulePanelTitle}
        onClose={closeRulePanel}
        // Escape closes a rule that is only being looked at; it never drops what is being typed.
        closeOnEscape={!showRuleForm && editingRuleInRow === null}
        labels={{
          panel: t("securityPage.ruleDetailPanel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <ListTable minWidth={640} testId="rules-list">
            <ListHead
              sortGroup={ruleSortGroup}
              columns={[
                { col: "id" },
                { col: "scope" },
                { col: "role" },
                { col: "filter" },
                { col: "description" },
              ]}
            />
            <Table.Tbody>
              {filtered.length === 0 && (
                <ListEmpty colSpan={5}>
                  {rules.length === 0
                    ? t("securityPage.noRulesDefined")
                    : t("securityPage.noRulesMatchFilter")}
                </ListEmpty>
              )}
              <ListItems
                state={ruleSortGroup}
                colSpan={5}
                rowKey={(r) => r.id}
                render={(r) => (
                  <React.Fragment>
                    <ListRow
                      // The row whose detail the right-hand panel is showing.
                      selected={expandedRule === r.id}
                      onClick={() => {
                        // The row opens its detail in the panel, swaps it, or closes it.
                        setExpandedRule(expandedRule === r.id ? null : r.id);
                        setEditingRuleInRow(null);
                        setShowRuleForm(false);
                      }}
                    >
                      <Table.Td>{r.id}</Table.Td>
                      <Table.Td>
                        {r.tagId ? (
                          <span data-testid={`rule-scope-${r.id}`}>
                            <Text span c="dimmed" fz="0.75em">
                              {t("securityPage.tagPrefix")}{" "}
                            </Text>
                            {r.tagId}
                          </span>
                        ) : r.actionName ? (
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
                      <Table.Td data-testid={`rule-description-${r.id}`}>{r.description}</Table.Td>
                    </ListRow>
                  </React.Fragment>
                )}
              />
            </Table.Tbody>
          </ListTable>
        }
      >
        {rulePanelBody}
      </SplitPane>
    </Stack>
  );
}
