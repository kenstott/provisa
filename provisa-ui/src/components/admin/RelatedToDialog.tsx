// Copyright (c) 2026 Kenneth Stott
// Canary: 8b14e6d2-3f70-4c59-a2d8-6e91f0c5b37a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387 (CHOOSING RELATED_TO OPENS THE ALTERNATIVES): choosing Related to for an edge a person
// is adding interrupts, deliberately (an exception to the minimal-modal rule). The dialog says why
// Related to is the least useful edge, draws the five shapes that would resolve the pair, tightest
// first, each with a name the model suggests for the new term, and offers three acts: start a shape
// (the new-term form, prefilled with that name and both edges), add Related to anyway, or cancel.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Loader,
  Modal,
  MultiSelect,
  Stack,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import {
  RESOLVE_SHAPES,
  createGlossaryTerm,
  shapeEdges,
  suggestResolvingTerms,
  type ResolveShape,
} from "../../api/glossary";
import { GroundingGuideLink, ShapeLine } from "./relTypeGuide";

export type PairTerm = { id: number; name: string };
type Suggested = Partial<Record<ResolveShape, string[]>>;

/** The dialog opened by choosing Related to between ``from`` and ``to``. */
export function RelatedToDialog({
  from,
  to,
  onStart,
  onAnyway,
  onCancel,
}: {
  from: PairTerm;
  to: PairTerm;
  /** Start a shape: the new-term form, its name prefilled with the candidate when there is one. */
  onStart: (shape: ResolveShape, name: string) => void;
  onAnyway: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  // The model's names for this pair, or why it gave none: then the person types the name.
  const [suggested, setSuggested] = useState<Suggested | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    suggestResolvingTerms(from.id, to.id).then(
      ({ resolve }) => {
        if (live) setSuggested(Object.fromEntries(resolve.map((r) => [r.shape, r.candidates])));
      },
      (e: unknown) => {
        if (live) setFailed(e instanceof Error ? e.message : String(e));
      },
    );
    return () => {
      live = false;
    };
  }, [from.id, to.id]);

  return (
    <Modal
      opened
      onClose={onCancel}
      title={t("glossaryTab.relatedToDialogTitle", { from: from.name, to: to.name })}
      size="lg"
      data-testid="glossary-related-to-dialog"
    >
      <Stack gap="md">
        <Text size="sm">{t("glossaryTab.relatedToDialogWhy")}</Text>
        <Text size="sm" fw={500}>
          {t("glossaryTab.relatedToDialogShapes")}
        </Text>
        {suggested === null && failed === null && (
          <Group gap="xs" data-testid="glossary-related-to-suggesting">
            <Loader size="xs" />
            <Text size="xs" c="dimmed">
              {t("glossaryTab.relatedToDialogSuggesting")}
            </Text>
          </Group>
        )}
        {failed !== null && (
          <Alert color="gray" variant="light" data-testid="glossary-related-to-no-names">
            {t("glossaryTab.relatedToDialogNoNames", { reason: failed })}
          </Alert>
        )}
        {RESOLVE_SHAPES.map((shape) => {
          const name = suggested?.[shape]?.[0] ?? "";
          return (
            <Group
              key={shape}
              justify="space-between"
              wrap="nowrap"
              gap="sm"
              data-testid={`glossary-related-to-option-${shape}`}
            >
              <ShapeLine
                shape={shape}
                candidates={[name || t("glossaryTab.newTermPlaceholder")]}
                from={from.name}
                to={to.name}
              />
              <Button
                size="xs"
                variant="light"
                style={{ flexShrink: 0 }}
                disabled={suggested === null && failed === null}
                onClick={() => onStart(shape, name)}
                data-testid={`glossary-related-to-start-${shape}`}
              >
                {t("glossaryTab.relatedToDialogStart")}
              </Button>
            </Group>
          );
        })}
        <Group justify="space-between" wrap="wrap">
          <GroundingGuideLink testId="glossary-related-to-dialog-guide" />
          <Group gap="xs">
            <Button variant="default" onClick={onCancel} data-testid="glossary-related-to-cancel">
              {t("glossaryTab.cancel")}
            </Button>
            <Button variant="default" onClick={onAnyway} data-testid="glossary-related-to-anyway">
              {t("glossaryTab.relatedToDialogAnyway")}
            </Button>
          </Group>
        </Group>
      </Stack>
    </Modal>
  );
}

/** The new-term form a shape starts: the name prefilled, both edges shown for the person to confirm
 * or rename. The term is created on Create; ``onCreated`` places the edges. */
export function ShapeTermForm({
  shape,
  from,
  to,
  initialName,
  initialDomains,
  domainOptions,
  domainsEnabled,
  onCreated,
  onCancel,
}: {
  shape: ResolveShape;
  from: PairTerm;
  to: PairTerm;
  initialName: string;
  initialDomains: string[];
  domainOptions: { value: string; label: string }[];
  domainsEnabled: boolean;
  onCreated: (created: PairTerm) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState(initialName);
  const [definition, setDefinition] = useState("");
  const [domains, setDomains] = useState(initialDomains);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const shown = name.trim() || t("glossaryTab.newTermPlaceholder");
  const relText = (rel: string) => t(`glossaryTab.rel_${rel}`);
  const create = async () => {
    setSaving(true);
    setError("");
    try {
      const { id } = await createGlossaryTerm({
        name: name.trim(),
        ...(definition.trim() ? { definition: definition.trim() } : {}),
        ...(domainsEnabled ? { domains } : {}),
      });
      await onCreated({ id, name: name.trim() });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };
  return (
    <Modal
      opened
      onClose={onCancel}
      title={t("glossaryTab.shapeFormTitle", { shape: t(`glossaryTab.shape_${shape}`) })}
      size="lg"
      data-testid="glossary-shape-form"
    >
      <Stack gap="sm">
        <TextInput
          label={t("glossaryTab.nameLabel")}
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
          data-autofocus
          data-testid="glossary-shape-name"
        />
        <Textarea
          label={t("glossaryTab.definitionLabel")}
          value={definition}
          onChange={(e) => setDefinition(e.currentTarget.value)}
          autosize
          minRows={2}
          data-testid="glossary-shape-definition"
        />
        {domainsEnabled && (
          <MultiSelect
            label={t("glossaryTab.domainsLabel")}
            data={domainOptions}
            value={domains}
            onChange={setDomains}
            data-testid="glossary-shape-domains"
          />
        )}
        <Text size="sm" fw={500}>
          {t("glossaryTab.shapeFormEdges")}
        </Text>
        <Stack gap={2} data-testid="glossary-shape-edges">
          {shapeEdges(shape, from.name, to.name, shown).map((e) => (
            <Text size="sm" key={`${e.from}:${e.rel_type}:${e.to}`}>
              {e.from} · {relText(e.rel_type)} · {e.to}
            </Text>
          ))}
        </Stack>
        {error && (
          <Alert color="red" data-testid="glossary-shape-error">
            {error}
          </Alert>
        )}
        <Group justify="flex-end" gap="xs">
          <Button variant="default" onClick={onCancel}>
            {t("glossaryTab.cancel")}
          </Button>
          <Button
            onClick={() => void create()}
            loading={saving}
            disabled={!name.trim() || (domainsEnabled && domains.length === 0)}
            data-testid="glossary-shape-create"
          >
            {t("glossaryTab.create")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
