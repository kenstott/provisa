// Copyright (c) 2026 Kenneth Stott
// Canary: 9d24f8b1-63ca-4e07-a5b2-70fe1c3d8964
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import {
  Code,
  Grid,
  Group,
  NavLink,
  ScrollArea,
  Select,
  Stack,
  Switch,
  Text,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { useTranslation } from "react-i18next";
import {
  fetchRepoBranches,
  fetchRepoFile,
  fetchRepoFiles,
  fetchRepoHistory,
} from "../../api/environments";
import type { RepoCommit } from "../../api/environments";
import { ConfigDiffView } from "./ConfigDiffView";

function shortSha(sha: string): string {
  return sha.slice(0, 8);
}

/**
 * REQ-1524: BROWSE — the repository's branches, a branch's commits, and one file at one ref.
 *
 * The branch list is NOT the environment list: a branch outlives the environment that wrote it, so
 * a ref here may name an environment that has since been deleted. That is the point of reading
 * them from the repository rather than from the registry — an earlier state is loadable only
 * because its ref is still there.
 *
 * Paths and text are two calls because the tree is a whole model: rendering the file list from a
 * single call carrying every definition would download the org's model to draw a sidebar.
 */
export function RepoBrowser({ orgId }: { orgId: string }) {
  const { t } = useTranslation();
  const [branches, setBranches] = useState<string[]>([]);
  const [ref, setRef] = useState<string | null>(null);
  const [commits, setCommits] = useState<RepoCommit[]>([]);
  // The file list is kept WITH the ref it was read at. A ref change and the answer that follows it
  // do not land in the same render, so a list held on its own would briefly describe the previous
  // ref — and the file read below would ask for a path that ref no longer has, and be answered 404.
  const [tree, setTree] = useState<{ ref: string; paths: string[] } | null>(null);
  const [path, setPath] = useState<string | null>(null);
  const [text, setText] = useState("");
  // The second ref of a comparison. Off by default: the common act is reading one file at one ref.
  const [comparing, setComparing] = useState(false);
  const [against, setAgainst] = useState<string | null>(null);
  const [againstText, setAgainstText] = useState("");

  const fail = (err: Error) => notifications.show({ color: "red", message: err.message });

  useEffect(() => {
    fetchRepoBranches(orgId)
      .then((rows) => {
        setBranches(rows);
        setRef((current) => current ?? rows[0] ?? null);
      })
      .catch(fail);
  }, [orgId]);

  useEffect(() => {
    if (ref === null) return;
    fetchRepoHistory(orgId, ref).then(setCommits).catch(fail);
    fetchRepoFiles(orgId, ref)
      .then((rows) => {
        setTree({ ref, paths: rows });
        // A path selected under the previous ref may not exist under this one. The selection is
        // dropped rather than carried across.
        setPath((current) => (current !== null && rows.includes(current) ? current : null));
      })
      .catch(fail);
  }, [orgId, ref]);

  // A ref or a path that changes while a read is outstanding would otherwise let the older answer
  // land last and paint the wrong file's text. `live` is what keeps the answer that arrives late
  // from overwriting the one that was asked for second.
  useEffect(() => {
    if (ref === null || path === null) return;
    // Not until the file list belongs to this ref: until then the path is the previous ref's.
    if (tree === null || tree.ref !== ref) return;
    let live = true;
    fetchRepoFile(orgId, ref, path)
      .then((body) => {
        if (live) setText(body);
      })
      .catch(fail);
    return () => {
      live = false;
    };
  }, [orgId, ref, path, tree]);

  useEffect(() => {
    if (!comparing || against === null || path === null) return;
    let live = true;
    fetchRepoFile(orgId, against, path)
      .then((body) => {
        if (live) setAgainstText(body);
      })
      .catch(fail);
    return () => {
      live = false;
    };
  }, [orgId, against, path, comparing]);

  const refOptions = branches.concat(
    // A commit is a ref too, and picking one is how an earlier state is read.
    commits.map((c) => c.sha),
  );

  return (
    <Stack gap="sm" data-testid="repo-browser">
      <Title order={4}>{t("environmentsTab.repoTitle")}</Title>
      <Group align="end">
        <Select
          label={t("environmentsTab.refLabel")}
          data={refOptions}
          value={ref}
          onChange={setRef}
          searchable
          data-testid="repo-ref-select"
        />
        <Switch
          label={t("environmentsTab.compare")}
          checked={comparing}
          onChange={(e) => setComparing(e.currentTarget.checked)}
          data-testid="repo-compare-toggle"
        />
        {comparing && (
          <Select
            label={t("environmentsTab.againstLabel")}
            data={refOptions}
            value={against}
            onChange={setAgainst}
            searchable
            data-testid="repo-against-select"
          />
        )}
      </Group>
      <Grid>
        <Grid.Col span={4}>
          <Text size="sm" fw={600}>
            {t("environmentsTab.commits")}
          </Text>
          <ScrollArea h={160}>
            {commits.map((c) => (
              <NavLink
                key={c.sha}
                active={ref === c.sha}
                onClick={() => setRef(c.sha)}
                data-testid={`repo-commit-${shortSha(c.sha)}`}
                label={
                  <Text size="xs">
                    <Code>{shortSha(c.sha)}</Code> {c.message}
                  </Text>
                }
                description={c.author}
              />
            ))}
          </ScrollArea>
          <Text size="sm" fw={600} mt="sm">
            {t("environmentsTab.files")}
          </Text>
          <ScrollArea h={240}>
            {(tree === null ? [] : tree.paths).map((p) => (
              <NavLink
                key={p}
                active={p === path}
                label={p}
                onClick={() => setPath(p)}
                data-testid={`repo-path-${p}`}
              />
            ))}
          </ScrollArea>
        </Grid.Col>
        <Grid.Col span={8}>
          {path === null ? (
            <Text c="dimmed" data-testid="repo-no-file">
              {t("environmentsTab.pickFile")}
            </Text>
          ) : comparing ? (
            <ConfigDiffView
              original={against === null ? "" : againstText}
              current={text}
              readOnly
            />
          ) : (
            <ScrollArea h={420}>
              <Code block data-testid="repo-file-text">
                {text}
              </Code>
            </ScrollArea>
          )}
        </Grid.Col>
      </Grid>
    </Stack>
  );
}
