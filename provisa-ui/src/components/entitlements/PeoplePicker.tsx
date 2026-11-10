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
import { Select } from "@mantine/core";
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
  const [debounced] = useDebouncedValue(search.trim(), 250);
  const [found, setFound] = useState<PersonMatch[]>([]);
  // The label of the chosen person, kept while the search moves on.
  const [chosen, setChosen] = useState<{ value: string; label: string } | null>(null);

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

  const typed = search.trim();
  // Under two characters the search is not run, and what it last found is not shown.
  const shown = debounced.length < 2 ? [] : found;
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
