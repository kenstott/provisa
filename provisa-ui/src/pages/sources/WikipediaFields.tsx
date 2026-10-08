// Copyright (c) 2026 Kenneth Stott
// Canary: 6a3d9e17-b84c-4f25-a7e1-5d0b2c8f9a63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1960: a Wikipedia source -- the pages to start from, how far to follow the links in their
// text, and every crawl setting, each with the brand's default shown until it is replaced.

import { Accordion, Checkbox, NumberInput, SimpleGrid, TextInput, Textarea } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { WIKIPEDIA_CRAWL_FIELDS, WIKIPEDIA_KNOWN_SEE_ALSO, wikipediaMissing } from "./wikipedia";

interface Props {
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

export function WikipediaFields({ fields, setFields }: Props) {
  const { t } = useTranslation();
  const missing = new Set(wikipediaMissing(fields));
  const set = (key: string, value: string) => setFields({ ...fields, [key]: value });
  const text = (key: string) => ({
    value: fields[key] ?? "",
    onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
      set(key, e.currentTarget.value),
    "data-testid": `wikipedia-${key}`,
  });
  const number = (key: string) => ({
    value: fields[key] === undefined || fields[key] === "" ? "" : Number(fields[key]),
    onChange: (value: string | number) => set(key, value === "" ? "" : String(value)),
    min: 0,
    "data-testid": `wikipedia-${key}`,
  });
  const language = (fields.wp_language ?? "").trim() || "en";
  const followSeeAlso = fields.wp_follow_see_also === "true";
  return (
    <>
      <Textarea
        label={t("sourceFormFieldsExtended.wpPages")}
        description={t("sourceFormFieldsExtended.wpPagesHelp")}
        placeholder={"List of tallest buildings\nhttps://en.wikipedia.org/wiki/Dubai"}
        required
        autosize
        minRows={2}
        error={missing.has("wp_pages") ? t("sourceFormFieldsExtended.wpPagesRequired") : undefined}
        style={{ gridColumn: "1 / -1" }}
        {...text("wp_pages")}
      />
      <TextInput
        label={t("sourceFormFieldsExtended.wpLanguage")}
        description={t("sourceFormFieldsExtended.wpLanguageHelp")}
        placeholder="en"
        {...text("wp_language")}
      />
      <NumberInput
        label={t("sourceFormFieldsExtended.wpMaxDepth")}
        description={t("sourceFormFieldsExtended.wpMaxDepthHelp")}
        placeholder="1"
        {...number("wp_max_depth")}
      />
      <NumberInput
        label={t("sourceFormFieldsExtended.wpMaxPages")}
        description={t("sourceFormFieldsExtended.wpMaxPagesHelp")}
        placeholder="25"
        {...number("wp_max_pages")}
      />
      <TextInput
        label={t("sourceFormFieldsExtended.wpContact")}
        description={t("sourceFormFieldsExtended.wpContactHelp")}
        placeholder="data-team@example.com"
        {...text("wp_contact")}
      />
      <Checkbox
        label={t("sourceFormFieldsExtended.wpFollowSeeAlso")}
        description={t("sourceFormFieldsExtended.wpFollowSeeAlsoHelp")}
        checked={followSeeAlso}
        onChange={(e) => set("wp_follow_see_also", e.currentTarget.checked ? "true" : "")}
        style={{ gridColumn: "1 / -1" }}
        data-testid="wikipedia-wp_follow_see_also"
      />
      {/* Asked only of an edition whose "See also" section Provisa cannot name, and in the
          operator's terms: the section's title as a page shows it. */}
      {!followSeeAlso && !WIKIPEDIA_KNOWN_SEE_ALSO.has(language) && (
        <TextInput
          label={t("sourceFormFieldsExtended.wpSeeAlsoHeading")}
          description={t("sourceFormFieldsExtended.wpSeeAlsoHeadingHelp")}
          placeholder="Voir aussi"
          required
          error={
            missing.has("wp_see_also_heading")
              ? t("sourceFormFieldsExtended.wpSeeAlsoHeadingRequired")
              : undefined
          }
          style={{ gridColumn: "1 / -1" }}
          {...text("wp_see_also_heading")}
        />
      )}
      <TextInput
        label={t("sourceFormFieldsExtended.wpDirectory")}
        description={t("sourceFormFieldsExtended.wpDirectoryHelp")}
        placeholder="/data/wikipedia"
        style={{ gridColumn: "1 / -1" }}
        {...text("wp_directory")}
      />
      {/* The crawl's own settings all have defaults: folded away until they are wanted. */}
      <Accordion
        variant="separated"
        style={{ gridColumn: "1 / -1" }}
        data-testid="wikipedia-crawl-settings"
      >
        <Accordion.Item value="crawl">
          <Accordion.Control>{t("sourceFormFieldsExtended.wpCrawlSettings")}</Accordion.Control>
          <Accordion.Panel>
            <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="md">
            {WIKIPEDIA_CRAWL_FIELDS.map((f) => {
              const key = `wpc_${f.key}`;
              const label = t(`sourceFormFieldsExtended.wpCrawl.${f.key}`);
              const description = t(`sourceFormFieldsExtended.wpCrawlHelp.${f.key}`);
              if (f.kind === "list") {
                return (
                  <Textarea
                    key={key}
                    label={label}
                    description={description}
                    placeholder={f.placeholder}
                    autosize
                    minRows={2}
                    {...text(key)}
                  />
                );
              }
              return f.kind === "number" ? (
                <NumberInput key={key} label={label} description={description} placeholder={f.placeholder} {...number(key)} />
              ) : (
                <TextInput key={key} label={label} description={description} placeholder={f.placeholder} {...text(key)} />
              );
            })}
            </SimpleGrid>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>
    </>
  );
}
