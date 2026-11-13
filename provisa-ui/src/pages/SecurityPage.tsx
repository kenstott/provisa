// Copyright (c) 2026 Kenneth Stott
// Canary: 09cc2288-9f68-4d9b-914e-1ba0f0e346d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { PageTitle } from "../components/PageTitle";
import { Tip } from "../components/Tip";
import React, { useState, useEffect, useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Pencil, Shield } from "lucide-react";
import {
  Alert,
  Button,
  Group,
  ActionIcon,
  Menu,
  NumberInput,
  Stack,
  Table,
  Tabs,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import { FilterInput } from "../components/admin/FilterInput";
import { HelpBubble } from "../components/HelpBubble";
import { MultiSelect } from "../components/MultiSelect";
import { useRoles, useDomains } from "../hooks/useAdminQueries";
import {
  useUpsertRole,
  useDeleteRole,
  useDeleteRoles,
  useRevokeRoleGrants,
} from "../hooks/useSecurityQueries";
import { RoleGrantAction } from "../components/RoleGrantAction";
import { removeUserAssignment } from "../api/admin";
import type { Role, Capability, SessionVar } from "../types/auth";
import { useDomainFilter } from "../context/DomainFilterContext";
import { assignableDomainIds } from "../lib/internalDomains";
import { PageLoading } from "../components/PageLoading";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { ResidencyGrant } from "../components/admin/ResidencyGrant";
import { RoleRead } from "./security/RoleRead";
import { LlmBudgetFields } from "./security/LlmBudget";
import { llmBudgetForm, llmBudgetInput } from "../lib/llmBudget";
import { RoleCapabilities } from "./security/RoleCapabilities";
import { RoleDataAccess } from "./security/RoleDataAccess";
import { SessionValuesField } from "./security/SessionValuesField";
import { CapabilityNames } from "./security/CapabilityNames";
import { CapabilityGrid, DomainsNote, SubtreeNote } from "./security/RoleFormParts";
import { parentRights, reachable, withParents } from "./security/roleParents";
import { useRegionChoices } from "../hooks/useRegionQueries";
import { ListTable, ListHead, ListRow, ListItems } from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { PanelActions, SplitPane } from "../components/list/SplitPane";
import { derivedId, readDerivedId, useDetailParam } from "../components/list/useDetailParam";
import { PanelDelete } from "../components/list/PanelDelete";
import { PanelDuplicate } from "../components/list/PanelDuplicate";
import { serverMessage } from "../i18n/serverMessage";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../components/form/FormLayout";
import { DerivedRoleDetail, DerivedRoleNewForm, DerivedRoleRows } from "./security/DerivedRoles";
import { useDerivedRoles, type DerivedPick } from "../hooks/useDerivedRoles";
import { matchesQuery } from "../lib/searchItems";

import { useListSelection } from "../components/list/listSelection";
import { ListBulkBar } from "../components/list/ListBulkBar";
/** A role may be saved once it lists a domain, or holds no capability to apply in one (REQ-2155). */
function listsADomain(form: { domainAccess: string[]; capabilities: Capability[] }): boolean {
  return form.domainAccess.length > 0 || form.capabilities.length === 0;
}

const EMPTY_ROLE = {
  id: "",
  description: "", // REQ-2080: required

  capabilities: [] as Capability[],
  domainAccess: [] as string[],
  parents: [] as string[], // REQ-2155: the roles it inherits from, in order
  residencyValues: [] as string[], // REQ-1921: with data_residency, the values it covers
  sessionVars: [] as SessionVar[], // REQ-1682: the role's constant session values
  // REQ-1174: per-role rate + query-complexity limits ("" = unlimited on that dimension).
  reqPerSec: "" as number | "",
  maxComplexity: "" as number | "",
  maxTimeMs: "" as number | "",
  llmBudget: llmBudgetForm(null), // REQ-1174: the per-call cap and rolling windows
  // REQ-2077: remove a grant of the role once unused this many days ("" = never).
  inactivityDays: "" as number | "",
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
  // REQ-2241: `?role=<id>&section=<tab>` is the open role's panel and its tab.
  const detail = useDetailParam("roles");
  const expandedRole = detail.id;
  const setExpandedRole = (id: string | null) => (id === null ? detail.close() : detail.open(id));
  const [editingRoleInRow, setEditingRoleInRow] = useState<string | null>(null);
  const [roleSearch, setRoleSearch] = useState("");
  // REQ-2078: derived roles share the list. The panel declares one, or shows the one picked.
  const { derived, problem: derivedProblem, reload: reloadDerived } = useDerivedRoles();
  const [showDerivedForm, setShowDerivedForm] = useState(false);
  // The derived role the declare form was opened from by Duplicate; null, it opens empty.
  const [derivedSeed, setDerivedSeed] = useState<{
    attribute: string;
    description: string;
  } | null>(null);
  // REQ-2241: `?derived=<attribute>:<value>` is the open derived role's panel (derivedId).
  const derivedDetail = useDetailParam("derived");
  const derivedPick: DerivedPick | null =
    derivedDetail.id === null ? null : readDerivedId(derivedDetail.id);
  const setDerivedPick = (pick: DerivedPick | null) =>
    pick === null
      ? derivedDetail.close()
      : derivedDetail.open(derivedId(pick.attribute, pick.value));

  const reload = useCallback(async () => {
    await Promise.all([refetchRoles(), refetchDomains(), reloadDerived()]);
  }, [refetchRoles, refetchDomains, reloadDerived]);

  useEffect(() => {
    setSelectedDomain("all");
  }, [setSelectedDomain]);

  useEffect(() => {
    setContextDomains(domains.map((x) => x.id));
  }, [domains, setContextDomains]);

  const handleNewRole = () => {
    setShowDerivedForm(false);
    setDerivedPick(null);
    setRoleForm({ ...EMPTY_ROLE });
    setShowRoleForm(true);
    setError("");
  };

  // Duplicate, from a role's panel head: the new-role form filled with the role's rights and
  // limits under "<id>-copy" (editable). Not copied: its members (who holds the role).
  const handleDuplicateRole = (role: Role) => {
    setShowDerivedForm(false);
    setDerivedPick(null);
    setEditingRoleInRow(null);
    startEditingRole(role);
    setEditingRoleInRow(null);
    setRoleForm((f) => ({ ...f, id: `${role.id}-copy` }));
    setShowRoleForm(true);
    setError("");
  };

  const handleSaveRole = async () => {
    if (!roleForm.id || !roleForm.description.trim()) return;
    setSaving(true);
    setError("");
    try {
      // REQ-1174: assemble the rate_limit input from the form; omit ("" → null) unset dimensions.
      const _n = (v: number | "") => (v === "" ? null : Number(v));
      const rateLimit = {
        requestsPerSecond: _n(roleForm.reqPerSec),
        maxQueryComplexity: _n(roleForm.maxComplexity),
        maxQueryTimeMs: _n(roleForm.maxTimeMs),
        ...llmBudgetInput(roleForm.llmBudget),
      };
      const { llmWindows, ...flat } = rateLimit;
      const hasLimit = [...Object.values(flat), ...Object.values(llmWindows)].some(
        (v) => v !== null,
      );
      const res = await upsertRole(
        {
          id: roleForm.id,
          description: roleForm.description.trim(), // REQ-2080
          capabilities: roleForm.capabilities,
          domainAccess: roleForm.domainAccess,
          rateLimit: hasLimit ? rateLimit : null,
          parents: roleForm.parents, // REQ-2155
          // REQ-1921: the grant's values go with the right; without it the role lists none.
          residencyValues: roleForm.capabilities.includes("data_residency")
            ? roleForm.residencyValues
            : [],
          sessionVars: roleForm.sessionVars, // REQ-1682
          inactivityDays: _n(roleForm.inactivityDays), // REQ-2077
        },
        // The new-role form creates; a role's own detail updates it.
        showRoleForm,
      );
      if (!res.success) {
        setError(serverMessage(res, res.message));
        return;
      }
      if (showRoleForm) {
        // The role exists now, with everything else at its defaults: it is selected in the list
        // and its detail opens in edit, where its capabilities and limits are set.
        setShowRoleForm(false);
        setExpandedRole(roleForm.id);
        setEditingRoleInRow(roleForm.id);
      } else {
        setRoleForm({ ...EMPTY_ROLE });
        setEditingRoleInRow(null);
      }
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  // REQ-2198: the panel's Delete (PanelDelete) shows the delete's progress and says why one
  // failed; it resolves once the list is read again without the role.
  const handleDeleteRole = async (id: string) => {
    setError("");
    const result = await deleteRole(id);
    if (refusal.refused(result, id)) return false;
    if (!result.success) throw new Error(serverMessage(result, result.message));
    await reload();
  };

  const startEditingRole = (role: Role) => {
    setRoleForm({
      id: role.id,
      description: role.description,
      capabilities: [...role.capabilities],
      domainAccess: [...role.domain_access],
      parents: [...(role.parents ?? [])], // REQ-2155
      residencyValues: [...(role.residencyValues ?? [])], // REQ-1921
      // REQ-1682: name and value only -- the input type takes no __typename.
      sessionVars: (role.sessionVars ?? []).map(({ name, value }) => ({ name, value })),
      reqPerSec: role.rateLimit?.requestsPerSecond ?? "",
      maxComplexity: role.rateLimit?.maxQueryComplexity ?? "",
      maxTimeMs: role.rateLimit?.maxQueryTimeMs ?? "",
      llmBudget: llmBudgetForm(role.rateLimit), // REQ-1174
      inactivityDays: role.inactivityDays ?? "", // REQ-2077
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

  // REQ-2155: the form offers what the parents hold; the rest is shown disabled.
  const held = parentRights(roleForm.parents, roles);
  const domainOptions = [
    { id: "*", label: t("securityPage.allDomains") },
    // REQ-1132: a system domain is held as a capability, never listed on a role.
    ...assignableDomainIds(domains.map((d) => d.id)).map((id) => ({ id, label: id })),
  ].map((option) => ({ ...option, disabled: !reachable(option.id, held) }));
  const chooseParents = (parents: string[]) => setRoleForm((f) => withParents(f, parents, roles));

  // REQ-1940: sort and group are the shared list mechanism.
  const filteredRoles = roles.filter((r) => matchesQuery([r.id, r.description], roleSearch));
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
    // REQ-2080: what each role is for.
    {
      key: "description",
      label: t("securityPage.colDescription"),
      sortValue: (r) => r.description,
    },
  ];
  const roleSortGroup = useListSortGroup(filteredRoles, roleColumns, "roles");
  // REQ-2240: roles selected and deleted together.
  const shownRoleIds = useMemo(() => filteredRoles.map((r) => r.id), [filteredRoles]);
  const selection = useListSelection(shownRoleIds);
  const deleteRoles = useDeleteRoles();

  if (loading) return <PageLoading message={t("securityPage.loadingRoles")} />;

  // The right-hand panel: the form that adds a role while it is open, otherwise the selected
  // role -- its detail, or the form that edits it. A role whose details are hidden from this user
  // has nothing to show, so it opens no panel.
  const closeRolePanel = () => {
    setShowDerivedForm(false);
    setDerivedPick(null);
    setShowRoleForm(false);
    setEditingRoleInRow(null);
    setExpandedRole(null);
  };
  const selectedRole =
    expandedRole === null
      ? null
      : (roles.find((x) => x.id === expandedRole && !x.detailsHidden) ?? null);
  const rolePanelTitle = showRoleForm
    ? t("securityPage.panelNewRole")
    : selectedRole
      ? selectedRole.id
      : "";
  // The first step of a new role asks only for what a role needs in order to exist: its id, and a
  // domain -- its own, or one a parent role hands down. It is a part of the form that edits a role.
  const rolePanelBody = showRoleForm ? (
    <FormLayout>
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("securityPage.roleId")}
            placeholder={t("securityPage.roleIdPlaceholder")}
            value={roleForm.id}
            onChange={(e) => setRoleForm({ ...roleForm, id: e.target.value })}
            data-testid="role-id-input"
          />
        </FormField>
      </FormIdentifiers>
      {/* REQ-2080: required. A role says what it is for, in words an access review reads. */}
      <FormField size="full" help={t("securityPage.roleDescriptionHelp")}>
        <Textarea
          label={t("securityPage.roleDescription")}
          required
          autosize
          minRows={2}
          value={roleForm.description}
          onChange={(e) => setRoleForm({ ...roleForm, description: e.target.value })}
          data-testid="role-description-input"
        />
      </FormField>
      <FormField size="long">
        <MultiSelect
          label={t("securityPage.domainAccess")}
          placeholder={t("securityPage.chooseDomains")}
          options={domainOptions}
          disabledHint={t("capabilityHelp.domainNotHeldByParent")}
          value={roleForm.domainAccess}
          onChange={(selected) => setRoleForm({ ...roleForm, domainAccess: selected })}
        />
        <SubtreeNote listed={roleForm.domainAccess} domains={domains} />
      </FormField>
      {!listsADomain(roleForm) && (
        <FormField size="full">
          <DomainsNote savedWithNone={false} />
        </FormField>
      )}
      {/* REQ-2155: several parents; the role holds what it is given of what they hold. */}
      <FormField help={t("securityPage.parentRoleHelp")} size="long">
        <div data-testid="role-parent-select">
          <MultiSelect
            label={t("securityPage.parentRole")}
            placeholder={t("securityPage.parentRoleNone")}
            options={roles
              .filter((r) => r.id !== roleForm.id)
              .map((r) => ({ id: r.id, label: r.id }))}
            value={roleForm.parents}
            onChange={chooseParents}
          />
        </div>
      </FormField>
      <FormFooter
        panel
        problem={error || undefined}
        onSave={handleSaveRole}
        saving={saving}
        onCancel={() => setShowRoleForm(false)}
        saveProps={{
          "data-testid": "save-role",
          disabled: !listsADomain(roleForm) || !roleForm.description.trim(),
        }}
        cancelProps={{ "data-testid": "cancel-role" }}
      />
    </FormLayout>
  ) : selectedRole ? (
    ((r) => (
      <>
        {/* Its actions sit in the panel's head; while its form is open the head is the form's. A
            standard role is Provisa's and locked (REQ-2155): it has none. */}
        {editingRoleInRow !== r.id && !r.standard && (
          <PanelActions>
            <fieldset className="panel-action-set">
              <Group gap="xs">
                <Tip text={t("securityPage.edit")} named>
                  <ActionIcon
                    aria-label={t("securityPage.edit")}
                    data-testid={`edit-role-${r.id}`}
                    onClick={(e) => {
                      e.stopPropagation();
                      startEditingRole(r);
                    }}
                  >
                    <Pencil size={14} />
                  </ActionIcon>
                </Tip>
                <PanelDuplicate
                  name={r.id}
                  onDuplicate={() => handleDuplicateRole(r)}
                  testId={`duplicate-role-${r.id}`}
                />
                <PanelDelete
                  itemKey={r.id}
                  title={t("securityPage.deleteRoleTitle", { id: r.id })}
                  consequence={t("securityPage.deleteRoleConsequence")}
                  ariaLabel={t("securityPage.delete")}
                  testId={`delete-role-${r.id}`}
                  onDelete={() => handleDeleteRole(r.id)}
                />
              </Group>
            </fieldset>
          </PanelActions>
        )}
        {editingRoleInRow !== r.id ? (
          // REQ-2161: the role's values, then its effective rights by dimension.
          <Tabs
            value={detail.tab ?? "details"}
            onChange={(v) => v && detail.setTab(v)}
            keepMounted={false}
          >
            <Tabs.List mb="sm" aria-label={t("roleRights.tabs")}>
              <Tabs.Tab value="details" data-testid={`role-tab-details-${r.id}`}>
                {t("roleRights.tab_details")}
              </Tabs.Tab>
              <Tabs.Tab value="capabilities" data-testid={`role-tab-capabilities-${r.id}`}>
                {t("roleRights.tab_capabilities")}
              </Tabs.Tab>
              <Tabs.Tab value="data" data-testid={`role-tab-data-${r.id}`}>
                {t("roleRights.tab_data")}
              </Tabs.Tab>
            </Tabs.List>
            <Tabs.Panel value="details">
              <RoleRead role={r} />
            </Tabs.Panel>
            <Tabs.Panel value="capabilities">
              <RoleCapabilities roleId={r.id} />
            </Tabs.Panel>
            <Tabs.Panel value="data">
              <RoleDataAccess roleId={r.id} />
            </Tabs.Panel>
          </Tabs>
        ) : (
          <FormLayout>
            {/* REQ-2080: required. A role says what it is for, in words an access review reads. */}
            <FormField size="full" help={t("securityPage.roleDescriptionHelp")}>
              <Textarea
                label={t("securityPage.roleDescription")}
                required
                autosize
                minRows={2}
                value={roleForm.description}
                onChange={(e) => setRoleForm({ ...roleForm, description: e.target.value })}
                data-testid="role-description-input"
              />
            </FormField>
            <CapabilityGrid
              value={roleForm.capabilities}
              held={held && held.capabilities}
              onToggle={toggleCapability}
              label={t("securityPage.capabilities")}
            />
            <ResidencyGrant
              held={roleForm.capabilities.includes("data_residency")}
              values={roleForm.residencyValues}
              regions={regionChoices.regions}
              parents={held}
              onHeld={() => toggleCapability("data_residency")}
              onValues={(residencyValues) => setRoleForm({ ...roleForm, residencyValues })}
            />
            <FormField size="long">
              <MultiSelect
                label={t("securityPage.domainAccess")}
                placeholder={t("securityPage.chooseDomains")}
                options={domainOptions}
                disabledHint={t("capabilityHelp.domainNotHeldByParent")}
                value={roleForm.domainAccess}
                onChange={(selected) => setRoleForm({ ...roleForm, domainAccess: selected })}
              />
              <SubtreeNote listed={roleForm.domainAccess} domains={domains} />
            </FormField>
            {!listsADomain(roleForm) && (
              <FormField size="full">
                <DomainsNote savedWithNone={r.domain_access.length === 0} />
              </FormField>
            )}
            <FormField size="long">
              <div data-testid={`role-parent-select-${r.id}`}>
                <MultiSelect
                  label={t("securityPage.parentRole")}
                  placeholder={t("securityPage.parentRoleNone")}
                  options={roles
                    .filter((x) => x.id !== r.id)
                    .map((x) => ({ id: x.id, label: x.id }))}
                  value={roleForm.parents}
                  onChange={chooseParents}
                />
              </div>
            </FormField>
            <FormField size="full">
              <SessionValuesField
                rows={roleForm.sessionVars}
                onChange={(sessionVars) => setRoleForm({ ...roleForm, sessionVars })}
              />
            </FormField>
            {/* REQ-1174: per-role rate + query-complexity limits. Blank = unlimited on that dimension. */}
            <FormField size="full">
              <Text size="sm" fw={600}>
                {t("securityPage.limitsHeading")}
              </Text>
            </FormField>
            <FormField size="short">
              <NumberInput
                label={t("securityPage.rateReqPerSec")}
                placeholder={t("securityPage.unlimited")}
                min={1}
                data-testid="role-req-per-sec"
                value={roleForm.reqPerSec}
                onChange={(v) =>
                  setRoleForm({ ...roleForm, reqPerSec: typeof v === "number" ? v : "" })
                }
              />
            </FormField>
            <FormField help={t("securityPage.maxQueryComplexityHint")} size="short">
              <NumberInput
                label={t("securityPage.maxQueryComplexity")}
                placeholder={t("securityPage.unlimited")}
                min={1}
                data-testid="role-max-complexity"
                value={roleForm.maxComplexity}
                onChange={(v) =>
                  setRoleForm({ ...roleForm, maxComplexity: typeof v === "number" ? v : "" })
                }
              />
            </FormField>
            <FormField size="short">
              <NumberInput
                label={t("securityPage.maxQueryTimeMs")}
                placeholder={t("securityPage.unlimited")}
                min={1}
                data-testid="role-max-time-ms"
                value={roleForm.maxTimeMs}
                onChange={(v) =>
                  setRoleForm({ ...roleForm, maxTimeMs: typeof v === "number" ? v : "" })
                }
              />
            </FormField>
            {/* REQ-2077: a grant of the role nobody uses this long is removed. Blank = never. */}
            <FormField help={t("securityPage.inactivityDaysHint")} size="short">
              <NumberInput
                label={t("securityPage.inactivityDays")}
                placeholder={t("securityPage.inactivityNever")}
                min={1}
                allowDecimal={false}
                data-testid="role-inactivity-days"
                value={roleForm.inactivityDays}
                onChange={(v) =>
                  setRoleForm({ ...roleForm, inactivityDays: typeof v === "number" ? v : "" })
                }
              />
            </FormField>
            <LlmBudgetFields
              value={roleForm.llmBudget}
              onChange={(llmBudget) => setRoleForm({ ...roleForm, llmBudget })}
            />
            <FormFooter
              panel
              problem={error || undefined}
              onSave={handleSaveRole}
              saving={saving}
              onCancel={() => setEditingRoleInRow(null)}
              saveProps={{
                "data-testid": `save-role-${r.id}`,
                disabled: !listsADomain(roleForm) || !roleForm.description.trim(),
              }}
            />
          </FormLayout>
        )}
      </>
    ))(selectedRole)
  ) : null;

  return (
    // The list and its panel share the height the heading leaves; each scrolls by itself.
    <Stack gap="md" p="md" className="page-sticky-head">
      {/* A form that did not save says so under the panel's head; this is for everything else. */}
      {error && !showRoleForm && editingRoleInRow === null && (
        <Alert color="red" data-testid="security-roles-error">
          {error}
        </Alert>
      )}
      {derivedProblem && (
        <Alert color="red" data-testid="derived-roles-error">
          {derivedProblem}
        </Alert>
      )}

      <Group justify="space-between" wrap="wrap">
        <PageTitle icon={Shield}>{t("securityPage.rolesHeading")}</PageTitle>
        <FilterInput
          value={roleSearch}
          onChange={setRoleSearch}
          placeholder={t("securityPage.filterByRoleId")}
        />
        {/* Nested Group: the parent spreads its children, and the bubble explains this
            button, so it has to travel with it. */}
        <Group gap="xs">
          {!showRoleForm && !showDerivedForm && (
            // REQ-2078: a role is granted rights; a derived one comes from an attribute.
            <Menu position="bottom-end" withinPortal>
              <Menu.Target>
                <Button data-testid="toggle-role-form">{t("securityPage.addRole")}</Button>
              </Menu.Target>
              <Menu.Dropdown>
                <Menu.Item
                  data-testid="add-granted-role"
                  onClick={() => {
                    setExpandedRole(null);
                    handleNewRole();
                  }}
                >
                  {t("securityPage.addGrantedRole")}
                </Menu.Item>
                <Tip text={t("securityPage.derivedRolesHelp")} position="left">
                  <Menu.Item
                    data-testid="add-derived-role"
                    onClick={() => {
                      setExpandedRole(null);
                      setEditingRoleInRow(null);
                      setShowRoleForm(false);
                      setDerivedPick(null);
                      setDerivedSeed(null);
                      setShowDerivedForm(true);
                    }}
                  >
                    {t("securityPage.addDerivedRole")}
                  </Menu.Item>
                </Tip>
              </Menu.Dropdown>
            </Menu>
          )}
          <HelpBubble
            title={t("securityPage.rolePurposeTitle")}
            paragraphs={[t("securityPage.rolePurposeBody"), t("securityPage.rolePurposeAdd")]}
            ariaLabel={t("securityPage.rolePurposeAria")}
            testId="roles-purpose-help"
          />
        </Group>
      </Group>

      <SplitPane
        testId="roles-split"
        storageKey="provisa.roles.detailWidth"
        title={
          showDerivedForm
            ? t("securityPage.panelNewDerivedRole")
            : derivedPick
              ? derivedPick.value === null
                ? derivedPick.attribute
                : `${derivedPick.attribute}__${derivedPick.value}`
              : rolePanelTitle
        }
        onClose={closeRolePanel}
        // Escape closes a role that is only being looked at; it never drops what is being typed.
        closeOnEscape={!showRoleForm && editingRoleInRow === null}
        labels={{
          panel: t("securityPage.roleDetailPanel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <>
            <ListBulkBar
              selection={selection}
              nameOf={(id) => id}
              onDelete={deleteRoles}
              testId="roles-bulk"
            />
            <ListTable minWidth={480} testId="roles-list" selection={selection}>
              <ListHead
                sortGroup={roleSortGroup}
                columns={[
                  { col: "id" },
                  { col: "capabilities" },
                  { col: "domains" },
                  { col: "description" },
                ]}
              />
              <Table.Tbody>
                <ListItems
                  state={roleSortGroup}
                  colSpan={4}
                  rowKey={(r) => r.id}
                  render={(r) => (
                    <React.Fragment>
                      <ListRow
                        // The row whose detail the right-hand panel is showing.
                        selected={selectedRole?.id === r.id}
                        onClick={() => {
                          // The row opens its detail in the panel, swaps it, or closes it.
                          setExpandedRole(expandedRole === r.id ? null : r.id);
                          setEditingRoleInRow(null);
                          setShowRoleForm(false);
                          setShowDerivedForm(false);
                          setDerivedPick(null);
                        }}
                      >
                        <Table.Td>{r.id}</Table.Td>
                        {r.detailsHidden ? (
                          <Table.Td
                            colSpan={2}
                            c="dimmed"
                            data-testid={`role-details-hidden-${r.id}`}
                          >
                            {t("securityPage.detailsHidden")}
                          </Table.Td>
                        ) : (
                          <>
                            <Table.Td>
                              <CapabilityNames
                                capabilities={r.capabilities}
                                regions={r.residencyValues}
                              />
                            </Table.Td>
                            <Table.Td data-testid={`role-domains-${r.id}`}>
                              {r.domain_access.join(", ") || t("securityPage.noDomains")}
                            </Table.Td>
                          </>
                        )}
                        <Table.Td data-testid={`role-description-${r.id}`}>
                          {r.description}
                        </Table.Td>
                      </ListRow>
                    </React.Fragment>
                  )}
                />
                {derived && (
                  <DerivedRoleRows
                    roles={derived.roles}
                    search={roleSearch}
                    picked={derivedPick}
                    onPick={(pick) => {
                      const same =
                        derivedPick?.attribute === pick.attribute &&
                        derivedPick.value === pick.value;
                      setDerivedPick(same ? null : pick);
                      setExpandedRole(null);
                      setEditingRoleInRow(null);
                      setShowRoleForm(false);
                      setShowDerivedForm(false);
                    }}
                  />
                )}
              </Table.Tbody>
            </ListTable>
          </>
        }
      >
        {showDerivedForm ? (
          <DerivedRoleNewForm
            key={derivedSeed?.attribute ?? "new"}
            seed={derivedSeed ?? undefined}
            onCancel={() => setShowDerivedForm(false)}
            onSaved={async (attribute) => {
              await reloadDerived();
              setShowDerivedForm(false);
              setDerivedPick({ attribute, value: null });
            }}
          />
        ) : derivedPick && derived ? (
          ((d) =>
            d ? (
              <DerivedRoleDetail
                key={`${d.attribute}:${derivedPick.value ?? ""}`}
                role={d}
                value={derivedPick.value}
                onChanged={reloadDerived}
                onDuplicate={() => {
                  setDerivedSeed({ attribute: `${d.attribute}_copy`, description: d.description });
                  setDerivedPick(null);
                  setExpandedRole(null);
                  setShowRoleForm(false);
                  setShowDerivedForm(true);
                }}
                onDeleted={async () => {
                  await reloadDerived();
                  setDerivedPick(null);
                }}
              />
            ) : null)(derived.roles.find((x) => x.attribute === derivedPick.attribute))
        ) : (
          rolePanelBody
        )}
      </SplitPane>
      {refusal.dialog}
    </Stack>
  );
}

export { SecurityRolesPage as SecurityPage };
