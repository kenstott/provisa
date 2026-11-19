// Copyright (c) 2026 Kenneth Stott
// Canary: 8a4f1c62-d3e9-47b5-b8a0-6c2e9f17d354
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: a relationship's links, one per surface that reads it, each opening that surface's
// Explore editor with the relationship's query, run at once, and each named by its surface. In the
// relationship's detail panel, where they stand under their own heading, each shows its surface's
// name beside its glyph; in a data product's Access section they are drawn as a table's links are
// there, an icon button whose tip names the surface.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Button, Group, Text } from "@mantine/core";
import {
  Braces,
  Cable,
  FileJson,
  Globe,
  Share2,
  Table as TableIcon,
  Waypoints,
} from "lucide-react";
import { Tip } from "../Tip";
import {
  relationshipQueryLinks,
  type QueryLink,
  type RelationshipQueryRef,
  type RelationshipSurface,
  type RelationshipSurfaces,
} from "./relationshipQueries";

/** One query link: an icon button with its tip, as the Access section draws each. */
export function QueryLinkIcon({
  label,
  testId,
  onClick,
  children,
}: {
  label: string;
  testId: string;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <Tip text={label} named>
      <ActionIcon
        variant="subtle"
        size="sm"
        aria-label={label}
        data-testid={testId}
        onClick={onClick}
      >
        {children}
      </ActionIcon>
    </Tip>
  );
}

const GLYPH: Record<RelationshipSurface, ReactNode> = {
  cypher: <Share2 size={14} />,
  sql: <TableIcon size={14} />,
  sparql: <Waypoints size={14} />,
  graphql: <Braces size={14} />,
  rest: <Globe size={14} />,
  grpc: <Cable size={14} />,
  jsonapi: <FileJson size={14} />,
};

/** One query link named by its surface: its glyph and the surface's name, with its tip. */
function QueryLinkButton({
  label,
  text,
  testId,
  onClick,
  glyph,
}: {
  label: string;
  text: string;
  testId: string;
  onClick: () => void;
  glyph: ReactNode;
}) {
  return (
    <Tip text={label} named>
      <Button
        variant="subtle"
        size="compact-xs"
        leftSection={glyph}
        aria-label={label}
        data-testid={testId}
        onClick={onClick}
      >
        {text}
      </Button>
    </Tip>
  );
}

export function RelationshipQueryLinks({
  rel,
  name,
  surfaces,
  heading,
  testIdPrefix,
}: {
  rel: RelationshipQueryRef;
  /** What the tips call the relationship: the name the panel shows it by. */
  name: string;
  surfaces: RelationshipSurfaces;
  /** Shown before the links, where they stand on their own (the detail panel). */
  heading?: string;
  testIdPrefix: string;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const links = relationshipQueryLinks(rel, surfaces);
  if (links.length === 0) return null;
  const open = (link: QueryLink) => navigate(link.to, { state: link.state });
  const named = heading !== undefined;
  return (
    <Group
      gap={4}
      wrap={named ? "wrap" : "nowrap"}
      mb={heading !== undefined ? "md" : 0}
      data-testid={`${testIdPrefix}-query-links`}
    >
      {heading !== undefined && (
        <Text component="span" fz="sm" c="dimmed" mr={4}>
          {heading}
        </Text>
      )}
      {links.map(({ surface, link }) => {
        const label = t(`relationshipRow.queryLinks.${surface}`, { name });
        const testId = `${testIdPrefix}-query-${surface}`;
        return named ? (
          <QueryLinkButton
            key={surface}
            label={label}
            text={t(`relationshipRow.queryLinks.surfaces.${surface}`)}
            testId={testId}
            onClick={() => open(link)}
            glyph={GLYPH[surface]}
          />
        ) : (
          <QueryLinkIcon key={surface} label={label} testId={testId} onClick={() => open(link)}>
            {GLYPH[surface]}
          </QueryLinkIcon>
        );
      })}
    </Group>
  );
}
