// Copyright (c) 2026 Kenneth Stott
// Canary: 8347d8cb-40cd-47b1-b395-337c672cd4d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  Alert,
  Badge,
  Button,
  Code,
  Group,
  NumberInput,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FormField, FormFooter, FormLayout } from "./form/FormLayout";
import {
  issuePersonalAccessToken,
  listPersonalAccessTokens,
  revokePersonalAccessToken,
  type PersonalAccessToken,
} from "../api/admin";
import { CopyButton } from "./CopyButton";
import { MultiSelect } from "./MultiSelect";
import { useAuth } from "../context/AuthContext";
import { selectedEnv } from "../lib/authFetch";

/**
 * REQ-1263: the user's own personal access tokens — the credential every non-browser protocol
 * accepts (pgwire, Bolt, Flight, gRPC, MCP, HTTP). Minting and revoking one is the token holder's
 * act, so it lives on their profile rather than under an admin page, alongside leaving an org and
 * deleting the account.
 *
 * A token's scopes are the roles it may act as: picked here from the roles the user holds, at
 * least one, and shown for each token. The server issues a token only for roles its issuer holds.
 *
 * The minted secret is displayed once, here, because the server stores only its SHA-256 and can
 * never show it again. Everything the listing carries is non-secret: a display prefix, the name
 * the user gave it, and its lifecycle timestamps.
 */
export function PersonalAccessTokens() {
  const { t, i18n } = useTranslation();
  const { availableRoles } = useAuth();
  const [tokens, setTokens] = useState<PersonalAccessToken[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [scopes, setScopes] = useState<string[]>([]);
  const [expiresInDays, setExpiresInDays] = useState<number | "">("");
  const [issuing, setIssuing] = useState(false);
  const [minted, setMinted] = useState<string | null>(null);

  const load = useCallback(
    () =>
      listPersonalAccessTokens()
        .then(setTokens)
        .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e))),
    [],
  );

  useEffect(() => {
    load();
  }, [load]);

  async function handleIssue() {
    setIssuing(true);
    setError(null);
    try {
      const created = await issuePersonalAccessToken({
        name: name.trim(),
        scopes,
        expires_in_days: expiresInDays === "" ? null : expiresInDays,
      });
      setMinted(created.token);
      setName("");
      setScopes([]);
      setExpiresInDays("");
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setIssuing(false);
    }
  }

  async function handleRevoke(tokenHash: string) {
    setError(null);
    try {
      await revokePersonalAccessToken(tokenHash);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function when(value: string | null): string {
    if (!value) return t("userProfileModal.patNever");
    return new Date(value).toLocaleDateString(i18n.language);
  }

  return (
    <>
      {error && (
        <Alert color="red" mb="xs" data-testid="profile-pat-error">
          {error}
        </Alert>
      )}

      <Text fz="0.85rem" c="dimmed" mb="xs">
        {t("userProfileModal.patHelp")}
      </Text>

      {minted && (
        <Alert color="yellow" mb="xs" data-testid="profile-pat-minted">
          <Text fz="0.8rem" mb={4}>
            {t("userProfileModal.patMintedHelp")}
          </Text>
          <Group gap="xs" wrap="nowrap">
            <Code data-testid="profile-pat-secret" style={{ wordBreak: "break-all" }}>
              {minted}
            </Code>
            <CopyButton text={minted} />
          </Group>
          <Button
            size="compact-xs"
            variant="default"
            mt="xs"
            onClick={() => setMinted(null)}
            data-testid="profile-pat-dismiss"
          >
            {t("userProfileModal.patDismiss")}
          </Button>
        </Alert>
      )}

      {/* REQ-1503: a token addresses the environment it is issued into, so the one a new token
          will address is said before it is issued. */}
      <Text fz="0.85rem" c="dimmed" mb="xs" data-testid="profile-pat-env-help">
        {t("userProfileModal.patEnvironmentHelp", { env: selectedEnv() ?? "prod" })}
      </Text>
      <FormLayout style={{ marginBottom: "0.75rem" }}>
        <FormField size="medium">
          <TextInput
            label={t("userProfileModal.patName")}
            value={name}
            onChange={(e) => setName(e.currentTarget.value)}
            data-testid="profile-pat-name"
          />
        </FormField>
        <FormField size="medium" hint={t("userProfileModal.patRolesHelp")}>
          <MultiSelect
            label={t("userProfileModal.patRoles")}
            placeholder={t("userProfileModal.patRolesPlaceholder")}
            options={availableRoles.map((r) => ({ id: r.id, label: r.id }))}
            value={scopes}
            onChange={setScopes}
            required
          />
        </FormField>
        <FormField size="short">
          <NumberInput
            label={t("userProfileModal.patExpiresInDays")}
            value={expiresInDays}
            onChange={(v) => setExpiresInDays(typeof v === "number" ? v : "")}
            min={1}
            max={366}
            data-testid="profile-pat-expiry"
          />
        </FormField>
        <FormFooter
          action={
            <Button
              disabled={!name.trim() || scopes.length === 0 || issuing}
              onClick={handleIssue}
              data-testid="profile-pat-issue"
            >
              {issuing ? t("userProfileModal.patIssuing") : t("userProfileModal.patIssue")}
            </Button>
          }
        />
      </FormLayout>

      {tokens && tokens.length === 0 ? (
        <Text fz="0.85rem" c="dimmed">
          {t("userProfileModal.patNone")}
        </Text>
      ) : (
        <Table fz="0.82rem" withTableBorder={false}>
          <Table.Thead>
            <Table.Tr>
              <Table.Th c="dimmed" fw={500}>
                {t("userProfileModal.patName")}
              </Table.Th>
              <Table.Th c="dimmed" fw={500}>
                {t("userProfileModal.patPrefix")}
              </Table.Th>
              <Table.Th c="dimmed" fw={500}>
                {t("userProfileModal.patRoles")}
              </Table.Th>
              <Table.Th c="dimmed" fw={500}>
                {t("userProfileModal.patLastUsed")}
              </Table.Th>
              <Table.Th c="dimmed" fw={500}>
                {t("userProfileModal.patExpires")}
              </Table.Th>
              <Table.Th c="dimmed" fw={500}>
                {t("userProfileModal.patEnvironment")}
              </Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {(tokens ?? []).map((tok) => (
              <Table.Tr key={tok.token_hash} data-testid={`profile-pat-row-${tok.prefix}`}>
                <Table.Td>{tok.name}</Table.Td>
                <Table.Td>
                  <Code>{tok.prefix}…</Code>
                </Table.Td>
                <Table.Td data-testid={`pat-roles-${tok.token_hash}`}>
                  {tok.scopes.join(", ")}
                </Table.Td>
                <Table.Td>{when(tok.last_used_at)}</Table.Td>
                <Table.Td>{when(tok.expires_at)}</Table.Td>
                <Table.Td data-testid={`pat-env-${tok.token_hash}`}>{tok.env_name}</Table.Td>
                <Table.Td ta="right">
                  {tok.revoked_at ? (
                    <Badge
                      size="xs"
                      color="gray"
                      // The table sizes this column to the word's fractional width and the label
                      // clips past its box, which cut the word by a pixel into an ellipsis.
                      styles={{ label: { overflow: "visible" } }}
                      data-testid={`profile-pat-revoked-${tok.prefix}`}
                    >
                      {t("userProfileModal.patRevoked")}
                    </Badge>
                  ) : (
                    <Button
                      size="compact-xs"
                      variant="default"
                      onClick={() => handleRevoke(tok.token_hash)}
                      data-testid={`profile-pat-revoke-${tok.prefix}`}
                    >
                      {t("userProfileModal.patRevoke")}
                    </Button>
                  )}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </>
  );
}
