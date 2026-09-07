// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2a2f2e-6a2f-4a1f-8e1a-2c7a9f0b7a3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Loader, Popover, Stack, Text } from "@mantine/core";
import { Users } from "lucide-react";
import { useResolveOwners } from "../hooks/useAdminQueries";
import type { UserSummary } from "../types/admin";

function summaryLabel(u: UserSummary): string {
  return u.displayName || u.email || u.userId;
}

// REQ-609/REQ-1634: an icon button that, on click, resolves a role/user ref list (an
// owner_role, a domain steward, or a column's visible_to) to the individuals it grants
// rights to. Renders nothing when refs is empty — callers gate public columns themselves.
export function OwnerResolutionIcon({
  refs,
  ariaLabel,
}: {
  refs: string[];
  ariaLabel?: string;
}) {
  const { t } = useTranslation();
  const resolveOwners = useResolveOwners();
  const [opened, setOpened] = useState(false);
  const [loading, setLoading] = useState(false);
  const [members, setMembers] = useState<UserSummary[] | null>(null);

  if (refs.length === 0) return null;

  const label = ariaLabel ?? t("ownerResolution.iconLabel");

  return (
    <Popover
      opened={opened}
      onChange={setOpened}
      withArrow
      shadow="md"
      trapFocus
      onClose={() => setOpened(false)}
    >
      <Popover.Target>
        <ActionIcon
          variant="subtle"
          size="sm"
          aria-label={label}
          onClick={async (e) => {
            e.stopPropagation();
            const next = !opened;
            setOpened(next);
            if (next && members === null) {
              setLoading(true);
              try {
                setMembers(await resolveOwners(refs));
              } finally {
                setLoading(false);
              }
            }
          }}
        >
          <Users size={14} />
        </ActionIcon>
      </Popover.Target>
      <Popover.Dropdown onClick={(e) => e.stopPropagation()}>
        {loading ? (
          <Loader size="xs" />
        ) : (
          <Stack gap={2}>
            {members && members.length > 0 ? (
              members.map((m) => (
                <Text key={m.userId} size="xs">
                  {summaryLabel(m)}
                </Text>
              ))
            ) : (
              <Text size="xs" c="var(--text-muted)">
                {t("ownerResolution.empty")}
              </Text>
            )}
          </Stack>
        )}
      </Popover.Dropdown>
    </Popover>
  );
}

// REQ-609/REQ-1634: subheading variant for detail panels — resolves eagerly (mounted only when
// refs is non-empty) and renders the individuals inline under the owner/steward row.
export function OwnerResolutionInline({ refs }: { refs: string[] }) {
  const { t } = useTranslation();
  const resolveOwners = useResolveOwners();
  // The answer is kept with the refs it was fetched for, so a refs change reads as "loading"
  // without a reset in the effect body, and a late answer for the old refs is never shown.
  const [resolved, setResolved] = useState<{ key: string; members: UserSummary[] } | null>(null);
  const key = refs.join(",");
  const members = resolved !== null && resolved.key === key ? resolved.members : null;

  useEffect(() => {
    const wanted = key.split(",").filter(Boolean);
    if (wanted.length === 0) return;
    let cancelled = false;
    resolveOwners(wanted).then((result) => {
      if (!cancelled) setResolved({ key, members: result });
    });
    return () => {
      cancelled = true;
    };
  }, [key, resolveOwners]);

  if (refs.length === 0) return null;

  if (members === null) {
    return (
      <Text size="xs" c="var(--text-muted)">
        {t("ownerResolution.loading")}
      </Text>
    );
  }

  if (members.length === 0) {
    return (
      <Text size="xs" c="var(--text-muted)">
        {t("ownerResolution.empty")}
      </Text>
    );
  }

  return (
    <Text size="xs" c="var(--text-muted)">
      {members.map(summaryLabel).join(", ")}
    </Text>
  );
}
