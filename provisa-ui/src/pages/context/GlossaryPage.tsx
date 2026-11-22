// Copyright (c) 2026 Kenneth Stott
// Canary: 9a4f2c71-5d8e-4b36-b1a9-e2c7d0f6385b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the Glossary page -- the approved terms read like a book's glossary: each headword,
// its other names, its plain-prose definition and its related terms as links within the page; a
// name defined differently per domain is one entry with numbered senses labelled by domain.

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { FormField, FormLayout } from "../../components/form/FormLayout";
import {
  Alert,
  Anchor,
  Badge,
  Group,
  Loader,
  MultiSelect,
  SegmentedControl,
  Stack,
  Text,
  TextInput,
  Title,
  UnstyledButton,
} from "@mantine/core";
import { BookA } from "lucide-react";
import { ENTERPRISE_DOMAIN, fetchGlossaryEntries } from "../../api/glossary";
import { PageTitle } from "../../components/PageTitle";
import { EntityLink } from "../../components/EntityLink";
import {
  buildEntries,
  domainsOf,
  filterEntries,
  LETTERS,
  letterAnchor,
  senseAnchor,
  sortEntries,
  type GlossaryEntry,
  type GlossarySense,
  type GlossarySort,
  type TermRef,
} from "./glossaryBook";

function scrollTo(id: string) {
  document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
}

export function GlossaryPage() {
  const { t } = useTranslation();
  const [entries, setEntries] = useState<GlossaryEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [domains, setDomains] = useState<string[]>([]);
  const [sort, setSort] = useState<GlossarySort>("name");
  // A related term the filters hide: they are cleared, and the page scrolls to it once drawn.
  const pending = useRef<number | null>(null);

  useEffect(() => {
    fetchGlossaryEntries().then(
      (data) => setEntries(buildEntries(data)),
      (e: Error) => setError(e.message),
    );
  }, []);

  const shown = useMemo(
    () => (entries === null ? [] : sortEntries(filterEntries(entries, search, domains), sort)),
    [entries, search, domains, sort],
  );
  const domainOptions = useMemo(() => (entries === null ? [] : domainsOf(entries)), [entries]);
  const lettersShown = useMemo(() => new Set(shown.map((e) => e.letter)), [shown]);

  useEffect(() => {
    if (pending.current === null) return;
    scrollTo(senseAnchor(pending.current));
    pending.current = null;
  }, [shown]);

  const goToTerm = (id: number) => {
    if (shown.some((e) => e.senses.some((s) => s.id === id))) {
      scrollTo(senseAnchor(id));
      return;
    }
    pending.current = id;
    setSearch("");
    setDomains([]);
  };

  if (error !== null) {
    return (
      <div className="page">
        <Alert color="red" data-testid="glossary-error">
          {t("glossaryPage.loadFailed", { message: error })}
        </Alert>
      </div>
    );
  }
  if (entries === null) {
    return (
      <div className="page">
        <Loader size="sm" />
      </div>
    );
  }

  const sections: { letter: string | null; entries: GlossaryEntry[] }[] =
    sort === "name"
      ? LETTERS.filter((l) => lettersShown.has(l)).map((l) => ({
          letter: l,
          entries: shown.filter((e) => e.letter === l),
        }))
      : [{ letter: null, entries: shown }];

  return (
    <div className="page" data-testid="glossary-page">
      <Stack gap="md" maw={880}>
        <div>
          <PageTitle icon={BookA}>{t("glossaryPage.title")}</PageTitle>
          <Text c="dimmed" size="sm" mt={4}>
            {t("glossaryPage.intro")}
          </Text>
        </div>
        <Group gap="sm" align="flex-end" wrap="wrap">
          <FormLayout style={{ flex: "1 1 auto" }}>
            <FormField size="medium">
              <TextInput
                label={t("glossaryPage.search")}
                placeholder={t("glossaryPage.searchPlaceholder")}
                value={search}
                onChange={(e) => setSearch(e.currentTarget.value)}
                data-testid="glossary-search"
              />
            </FormField>
            <FormField size="medium">
              <MultiSelect
                label={t("glossaryPage.domainFilter")}
                placeholder={
                  domains.length === 0 ? t("glossaryPage.domainFilterPlaceholder") : undefined
                }
                data={domainOptions}
                value={domains}
                onChange={setDomains}
                clearable
                searchable
                comboboxProps={{ withinPortal: true }}
                data-testid="glossary-domain-filter"
              />
            </FormField>
          </FormLayout>
          <Stack gap={4}>
            <Text size="sm" fw={500}>
              {t("glossaryPage.sort")}
            </Text>
            <SegmentedControl
              value={sort}
              onChange={(v) => setSort(v as GlossarySort)}
              data={[
                { value: "name", label: t("glossaryPage.sortName") },
                { value: "recent", label: t("glossaryPage.sortRecent") },
              ]}
              aria-label={t("glossaryPage.sort")}
              data-testid="glossary-sort"
            />
          </Stack>
        </Group>
        {sort === "name" && (
          <Group
            gap={4}
            role="navigation"
            aria-label={t("glossaryPage.letterBar")}
            data-testid="glossary-letters"
          >
            {LETTERS.map((l) => (
              <UnstyledButton
                key={l}
                disabled={!lettersShown.has(l)}
                onClick={() => scrollTo(letterAnchor(l))}
                aria-label={l === "#" ? t("glossaryPage.otherLetter") : l}
                data-testid={`glossary-letter-${l}`}
                px={6}
                fw={600}
                c={lettersShown.has(l) ? "blue" : "dimmed"}
                style={{ cursor: lettersShown.has(l) ? "pointer" : "default" }}
              >
                {l === "#" ? t("glossaryPage.otherLetter") : l}
              </UnstyledButton>
            ))}
          </Group>
        )}
        <Text size="sm" c="dimmed" data-testid="glossary-count">
          {t("glossaryPage.count", { count: shown.length })}
        </Text>
        {shown.length === 0 && (
          <Text data-testid="glossary-empty">
            {entries.length === 0 ? t("glossaryPage.empty") : t("glossaryPage.noMatch")}
          </Text>
        )}
        {sections.map((section) => (
          <Stack key={section.letter ?? "recent"} gap="sm">
            {section.letter !== null && (
              <Title
                order={3}
                id={letterAnchor(section.letter)}
                data-testid={`glossary-section-${section.letter}`}
                style={{
                  borderBottom: "1px solid var(--mantine-color-default-border)",
                  scrollMarginTop: 80,
                }}
              >
                {section.letter === "#" ? t("glossaryPage.otherLetter") : section.letter}
              </Title>
            )}
            {section.entries.map((entry) => (
              <EntryView key={entry.key} entry={entry} onTerm={goToTerm} />
            ))}
          </Stack>
        ))}
      </Stack>
    </div>
  );
}

function EntryView({ entry, onTerm }: { entry: GlossaryEntry; onTerm: (id: number) => void }) {
  const numbered = entry.senses.length > 1;
  return (
    <div data-testid={`glossary-entry-${entry.key}`}>
      <div>
        {numbered ? (
          <Text span fw={700} size="lg">
            {entry.name}
          </Text>
        ) : (
          <EntityLink
            kind="term"
            id={entry.senses[0].id}
            testId={`glossary-name-${entry.senses[0].id}`}
          >
            <Text span fw={700} size="lg" c="inherit">
              {entry.name}
            </Text>
          </EntityLink>
        )}
      </div>
      <Stack gap={6} mt={2}>
        {entry.senses.map((sense, i) => (
          <SenseView
            key={sense.id}
            sense={sense}
            number={numbered ? i + 1 : null}
            name={entry.name}
            onTerm={onTerm}
          />
        ))}
      </Stack>
    </div>
  );
}

function SenseView({
  sense,
  number,
  name,
  onTerm,
}: {
  sense: GlossarySense;
  number: number | null;
  name: string;
  onTerm: (id: number) => void;
}) {
  const { t } = useTranslation();
  const domainLabel = sense.domains
    .map((d) => (d === ENTERPRISE_DOMAIN ? t("glossaryPage.allDomains") : d))
    .join(", ");
  return (
    <div
      id={senseAnchor(sense.id)}
      data-testid={`glossary-sense-${sense.id}`}
      style={{ scrollMarginTop: 80 }}
    >
      {number !== null && (
        <Group gap={6} wrap="nowrap" align="baseline">
          <Text fw={600} data-testid={`glossary-sense-number-${sense.id}`}>
            {number}.
          </Text>
          {domainLabel !== "" && (
            <Badge
              variant="light"
              size="sm"
              tt="none"
              data-testid={`glossary-sense-domain-${sense.id}`}
            >
              <EntityLink kind="term" id={sense.id} testId={`glossary-name-${sense.id}`}>
                {domainLabel}
              </EntityLink>
            </Badge>
          )}
          {domainLabel === "" && (
            <EntityLink kind="term" id={sense.id} testId={`glossary-name-${sense.id}`}>
              {name}
            </EntityLink>
          )}
        </Group>
      )}
      {sense.alsoCalled.length > 0 && (
        <Related
          label={t("glossaryPage.alsoCalled")}
          terms={sense.alsoCalled}
          onTerm={onTerm}
          kind="also"
          id={sense.id}
        />
      )}
      {sense.definition && (
        <Text size="sm" mt={2} data-testid={`glossary-definition-${sense.id}`}>
          {sense.definition}
        </Text>
      )}
      {sense.kindOf.length > 0 && (
        <Related
          label={t("glossaryPage.kindOf")}
          terms={sense.kindOf}
          onTerm={onTerm}
          kind="kindof"
          id={sense.id}
        />
      )}
      {sense.kinds.length > 0 && (
        <Related
          label={t("glossaryPage.kinds")}
          terms={sense.kinds}
          onTerm={onTerm}
          kind="kinds"
          id={sense.id}
        />
      )}
      {sense.seeAlso.length > 0 && (
        <Related
          label={t("glossaryPage.seeAlso")}
          terms={sense.seeAlso}
          onTerm={onTerm}
          kind="seealso"
          id={sense.id}
        />
      )}
    </div>
  );
}

function Related({
  label,
  terms,
  onTerm,
  kind,
  id,
}: {
  label: string;
  terms: TermRef[];
  onTerm: (id: number) => void;
  kind: string;
  id: number;
}) {
  const links: ReactNode[] = [];
  terms.forEach((r, i) => {
    if (i > 0) links.push(", ");
    links.push(
      <Anchor
        key={r.id}
        href={`#${senseAnchor(r.id)}`}
        onClick={(e) => {
          e.preventDefault();
          onTerm(r.id);
        }}
        data-testid={`glossary-related-${r.id}`}
      >
        {r.name}
      </Anchor>,
    );
  });
  return (
    <Text size="sm" mt={2} data-testid={`glossary-${kind}-${id}`}>
      <Text span fs="italic" c="dimmed">
        {label}:
      </Text>{" "}
      {links}
    </Text>
  );
}
