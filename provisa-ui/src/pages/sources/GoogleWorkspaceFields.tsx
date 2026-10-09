// Copyright (c) 2026 Kenneth Stott
// Canary: 359ca4ed-1eb0-49fb-a4f7-decbb82154e1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.
// REQ-1923: a Google Workspace source -- whose mailbox, how much of it to read, which mail to
// hold, and one button to connect it. The person adding a source is asked nothing about how
// Provisa signs in to Google: the client is the organisation's, entered once by an
// administrator under Admin › Email, and a source keeps only its owner's approval.

import { useCallback, useEffect, useState } from "react";
import { Alert, Anchor, Button, Checkbox, Group, Select, TextInput, Textarea } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { serverMessage } from "../../i18n/serverMessage";
import {
  SignInRefused,
  signInSource,
  signInStatus,
  type SignInStatus,
} from "../../lib/sourceSignIn";
import {
  GOOGLE_WORKSPACE,
  GW_MAIL_FULL,
  GW_MAIL_HEADERS,
  gwByApproval,
  gwConnectMissing,
  gwHeadersOnly,
  gwMissing,
  gwScopes,
} from "./googleWorkspace";

interface Props {
  sourceId: string;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const WIDE = { gridColumn: "1 / -1" } as const;
/** Where an administrator enters the organisation's Google client. */
export const MAIL_PLATFORMS_ROUTE = "/admin/email";

export function GoogleWorkspaceFields({ sourceId, fields, setFields }: Props) {
  const { t } = useTranslation();
  const [status, setStatus] = useState<SignInStatus | null>(null);
  const [problem, setProblem] = useState("");
  const [connecting, setConnecting] = useState(false);
  const byApproval = gwByApproval(fields);
  const missing = new Set(gwMissing(fields));
  const refused = useCallback(
    (err: unknown) =>
      serverMessage(
        err instanceof SignInRefused
          ? { code: err.code, params: err.params, detail: err.message }
          : null,
        t("googleWorkspaceFields.connectFailed"),
      ),
    [t],
  );

  // Whether the organisation has connected Google Workspace decides what the form offers.
  useEffect(() => {
    if (!byApproval) return;
    let live = true;
    signInStatus(GOOGLE_WORKSPACE)
      .then((answered) => live && setStatus(answered))
      .catch((err: unknown) => live && setProblem(refused(err)));
    return () => {
      live = false;
    };
  }, [byApproval, refused]);

  const set = (key: string, value: string) => setFields({ ...fields, [key]: value });
  // A change to what Google was asked for is not what was approved: the approval is asked again.
  const setAsked = (key: string, value: string) =>
    setFields({ ...fields, [key]: value, gw_refresh_token: "", gw_connected_as: "" });
  const text = (key: string, change = set) => ({
    value: fields[key] ?? "",
    onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
      change(key, e.currentTarget.value),
    "data-testid": `google-workspace-${key}`,
  });

  const connect = async () => {
    setProblem("");
    setConnecting(true);
    try {
      const done = await signInSource({
        source_id: sourceId.trim(),
        kind: GOOGLE_WORKSPACE,
        account: fields.gw_account.trim(),
        scopes: gwScopes(fields),
      });
      setFields({
        ...fields,
        gw_refresh_token: done.refresh_token,
        gw_connected_as: done.account,
      });
    } catch (err) {
      setProblem(refused(err));
    } finally {
      setConnecting(false);
    }
  };

  const notConnected = byApproval && status !== null && !status.configured;
  return (
    <>
      <TextInput
        label={t("googleWorkspaceFields.account")}
        description={t("googleWorkspaceFields.accountHelp")}
        placeholder="ada@example.com"
        required
        style={WIDE}
        {...text("gw_account", setAsked)}
      />
      <Select
        label={t("googleWorkspaceFields.mailContent")}
        description={t("googleWorkspaceFields.mailContentHelp")}
        data={[
          { value: GW_MAIL_FULL, label: t("googleWorkspaceFields.mailContentFull") },
          { value: GW_MAIL_HEADERS, label: t("googleWorkspaceFields.mailContentHeaders") },
        ]}
        value={fields.gw_mail_content ?? null}
        onChange={(value) => setAsked("gw_mail_content", value ?? "")}
        required
        allowDeselect={false}
        style={WIDE}
        data-testid="google-workspace-gw_mail_content"
      />
      {notConnected && (
        <Alert
          color="yellow"
          variant="light"
          style={WIDE}
          data-testid="google-workspace-not-set-up"
        >
          {status.may_configure ? (
            <>
              {t("googleWorkspaceFields.notSetUp")}{" "}
              <Anchor
                component={Link}
                to={MAIL_PLATFORMS_ROUTE}
                data-testid="google-workspace-set-up"
              >
                {t("googleWorkspaceFields.setUpLink")}
              </Anchor>
            </>
          ) : (
            t("googleWorkspaceFields.notSetUpAsk")
          )}
        </Alert>
      )}
      {byApproval && !notConnected && (
        <Group style={WIDE} gap="sm" align="center">
          <Button
            type="button"
            onClick={connect}
            loading={connecting}
            disabled={status === null || gwConnectMissing(fields, sourceId).length > 0}
            data-testid="google-workspace-connect"
          >
            {t("googleWorkspaceFields.connect")}
          </Button>
          {fields.gw_connected_as && !missing.has("gw_refresh_token") && (
            <Alert
              color="green"
              variant="light"
              py={4}
              px="sm"
              data-testid="google-workspace-connected"
            >
              {t("googleWorkspaceFields.connected", { account: fields.gw_connected_as })}
            </Alert>
          )}
        </Group>
      )}
      {problem && (
        <Alert color="red" variant="light" style={WIDE} data-testid="google-workspace-problem">
          {problem}
        </Alert>
      )}
      {/* Google does not search mail it serves as headers and labels only. */}
      {!gwHeadersOnly(fields) && (
        <>
          <TextInput
            label={t("googleWorkspaceFields.mailSearch")}
            description={t("googleWorkspaceFields.mailSearchHelp")}
            placeholder="from:orders@example.com"
            style={WIDE}
            {...text("gw_mail_search")}
          />
          <TextInput
            type="date"
            label={t("googleWorkspaceFields.mailSince")}
            description={t("googleWorkspaceFields.mailSinceHelp")}
            {...text("gw_mail_since")}
          />
        </>
      )}
      <Textarea
        label={t("googleWorkspaceFields.mailLabels")}
        description={t("googleWorkspaceFields.mailLabelsHelp")}
        placeholder={"Clients\nInvoices"}
        autosize
        minRows={2}
        {...text("gw_mail_labels")}
      />
      <Checkbox
        label={t("googleWorkspaceFields.mailSpamTrash")}
        checked={fields.gw_mail_include_spam_trash === "true"}
        onChange={(e) => set("gw_mail_include_spam_trash", e.currentTarget.checked ? "true" : "")}
        style={WIDE}
        data-testid="google-workspace-gw_mail_include_spam_trash"
      />
    </>
  );
}
