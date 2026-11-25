// Copyright (c) 2026 Kenneth Stott
// Canary: 1a5d47dc-94e5-4946-bf55-a7547389f69f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219 (ONE API, THREE WAYS TO USE IT): the picker finds people through one search by name
// or email, over the org's local accounts and known members, and answers with the principal the
// sign-in token carries. A typed email that matches no one is offered as itself: the grant waits
// on it and attaches when a sign-in token's email matches.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { MultiSelect, Select } from "@mantine/core";
import { useDebouncedValue } from "@mantine/hooks";
import { searchPeople, type Grantee, type PersonMatch } from "../../api/entitlements";

const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PERSON = "p:";
const TYPED = "e:";

export interface PeoplePickerProps {
  label: string;
  /** Given by the FormField that places the picker. */
  description?: ReactNode;
  value: Grantee | null;
  onChange: (grantee: Grantee | null) => void;
  onError: (e: unknown) => void;
  testId?: string;
}

function personLabel(p: PersonMatch): string {
  if (p.name && p.email) return `${p.name} · ${p.email}`;
  return p.name ?? p.email ?? p.principal;
}

/** What the one search by name or email finds for ``search`` (under two characters, nothing). */
function usePeopleSearch(search: string, onError: (e: unknown) => void): PersonMatch[] {
  const [debounced] = useDebouncedValue(search.trim(), 250);
  const [found, setFound] = useState<PersonMatch[]>([]);

  useEffect(() => {
    if (debounced.length < 2) return;
    let live = true;
    searchPeople(debounced)
      .then((people) => {
        if (live) setFound(people);
      })
      .catch(onError);
    return () => {
      live = false;
    };
  }, [debounced, onError]);

  // Under two characters the search is not run, and what it last found is not shown.
  return debounced.length < 2 ? [] : found;
}

export function PeoplePicker({
  label,
  description,
  value,
  onChange,
  onError,
  testId = "people-picker",
}: PeoplePickerProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const shown = usePeopleSearch(search, onError);
  // The label of the chosen person, kept while the search moves on.
  const [chosen, setChosen] = useState<{ value: string; label: string } | null>(null);

  const typed = search.trim();
  const typedIsNew =
    EMAIL.test(typed) && !shown.some((p) => (p.email ?? "").toLowerCase() === typed.toLowerCase());
  const options = [
    ...shown.map((p) => ({ value: `${PERSON}${p.principal}`, label: personLabel(p) })),
    ...(typedIsNew
      ? [
          {
            value: `${TYPED}${typed}`,
            label: t("entitlements.people.typedEmail", { email: typed }),
          },
        ]
      : []),
  ];
  const selected =
    value === null
      ? null
      : "principal" in value
        ? `${PERSON}${value.principal}`
        : `${TYPED}${value.email}`;
  const data =
    chosen && selected === chosen.value && !options.some((o) => o.value === chosen.value)
      ? [chosen, ...options]
      : options;

  return (
    <Select
      label={label}
      description={description}
      inputWrapperOrder={["label", "input", "description"]}
      placeholder={t("entitlements.people.placeholder")}
      searchable
      clearable
      data={data}
      value={selected}
      searchValue={search}
      onSearchChange={setSearch}
      // The server has already matched name and email; the list shows what it found.
      filter={({ options: all }) => all}
      nothingFoundMessage={
        typed.length < 2 ? t("entitlements.people.typeMore") : t("entitlements.people.noneFound")
      }
      onChange={(v, option) => {
        if (v === null) {
          setChosen(null);
          onChange(null);
          return;
        }
        setChosen({ value: v, label: option.label });
        onChange(
          v.startsWith(PERSON)
            ? { principal: v.slice(PERSON.length) }
            : { email: v.slice(TYPED.length) },
        );
      }}
      data-testid={testId}
    />
  );
}

export interface PeopleMultiPickerProps {
  label: string;
  /** Given by the FormField that places the picker. */
  description?: ReactNode;
  /** The people picked, by the principal a sign-in token carries. */
  value: string[];
  onChange: (principals: string[]) => void;
  /**
   * The roles the field may name (those holding the kind's capability). Given, roles are offered
   * beside people, and a role picked is shown as a role chip: every holder of it is an owner or
   * steward (REQ-2219, amended 2026-11-23).
   */
  roleOptions?: string[];
  /** The roles picked, by id. */
  roles?: string[];
  onRolesChange?: (roles: string[]) => void;
  onError: (e: unknown) => void;
  testId?: string;
}

const ROLE = "r:";

/**
 * Several people, found by the same one search, and, when ``roleOptions`` is given, roles: an
 * Owner or Steward field (REQ-2219, amended 2026-11-23: those fields name people and roles). Only
 * people already known are offered -- a typed email names no one until a sign-in, and an owner
 * row names a person.
 */
export function PeopleMultiPicker({
  label,
  description,
  value,
  onChange,
  roleOptions,
  roles = [],
  onRolesChange,
  onError,
  testId = "people-multi-picker",
}: PeopleMultiPickerProps) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const shown = usePeopleSearch(search, onError);
  // Each picked person's label, kept while the search moves on; one picked before the form
  // opened is shown by its principal until a search finds them.
  const [labels, setLabels] = useState<Record<string, string>>({});
  const found = shown.map((p) => ({ value: `${PERSON}${p.principal}`, label: personLabel(p) }));
  const people = [
    ...value
      .filter((v) => !found.some((o) => o.value === `${PERSON}${v}`))
      .map((v) => ({ value: `${PERSON}${v}`, label: labels[v] ?? v })),
    ...found,
  ];
  const typed = search.trim();
  const roleItem = (r: string) => ({
    value: `${ROLE}${r}`,
    label: t("entitlements.people.roleChip", { role: r }),
  });
  // Roles are the org's own list: matched here by what is typed; a picked one always stays.
  const roleItems =
    roleOptions === undefined
      ? []
      : [
          ...roles.filter((r) => !roleOptions.includes(r)),
          ...roleOptions.filter(
            (r) => roles.includes(r) || r.toLowerCase().includes(typed.toLowerCase()),
          ),
        ].map(roleItem);
  const data =
    roleOptions === undefined
      ? people
      : [
          { group: t("entitlements.people.peopleGroup"), items: people },
          { group: t("entitlements.people.rolesGroup"), items: roleItems },
        ];
  const picked = [...value.map((v) => `${PERSON}${v}`), ...roles.map((r) => `${ROLE}${r}`)];

  return (
    <MultiSelect
      label={label}
      description={description}
      inputWrapperOrder={["label", "input", "description"]}
      placeholder={picked.length === 0 ? t("entitlements.people.placeholder") : undefined}
      searchable
      clearable
      data={data}
      value={picked}
      searchValue={search}
      onSearchChange={setSearch}
      // The server has already matched name and email; the list shows what it found.
      filter={({ options: all }) => all}
      nothingFoundMessage={
        typed.length < 2 ? t("entitlements.people.typeMore") : t("entitlements.people.noneFound")
      }
      onChange={(next) => {
        setLabels((held) => ({
          ...held,
          ...Object.fromEntries(
            found
              .filter((o) => next.includes(o.value))
              .map((o) => [o.value.slice(PERSON.length), o.label]),
          ),
        }));
        const nextPeople = next
          .filter((v) => v.startsWith(PERSON))
          .map((v) => v.slice(PERSON.length));
        const nextRoles = next.filter((v) => v.startsWith(ROLE)).map((v) => v.slice(ROLE.length));
        if (nextPeople.join("\n") !== value.join("\n")) onChange(nextPeople);
        if (onRolesChange && nextRoles.join("\n") !== roles.join("\n")) onRolesChange(nextRoles);
      }}
      comboboxProps={{ withinPortal: true }}
      data-testid={testId}
    />
  );
}
