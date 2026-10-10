// Copyright (c) 2026 Kenneth Stott
// Canary: 2c8f1b8c-cf98-4cd1-b373-375394da05b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.
// REQ-1923: a Microsoft 365 source -- whose mail, what is read, and one button. It has two
// shapes: one mailbox, which its owner connects, and the organisation's mailboxes (everyone, a
// group, or a list), read with the organisation's own credential once an administrator has
// allowed it. The person adding a source is asked nothing about how Provisa signs in to
// Microsoft: the client is the organisation's, entered once under Admin › Email.

import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Anchor,
  Button,
  Checkbox,
  Group,
  Select,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
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
  MAIL_PLATFORMS_ROUTE,
  MICROSOFT_365,
  M365_EVERYONE,
  M365_GROUP,
  M365_LIST,
  M365_ONE,
  M365_ORGANISATION,
  M365_SCOPES,
  m365CheckMailboxes,
  m365ConnectMissing,
  m365Mailboxes,
  m365Organisation,
} from "./microsoft365";

interface Props {
  sourceId: string;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const WIDE = { gridColumn: "1 / -1" } as const;

export function Microsoft365Fields({ sourceId, fields, setFields }: Props) {
  const { t } = useTranslation();
  const [status, setStatus] = useState<SignInStatus | null>(null);
  const [problem, setProblem] = useState("");
  const [connecting, setConnecting] = useState(false);
  const [checking, setChecking] = useState(false);
  const [found, setFound] = useState<number | null>(null);
  const organisation = m365Organisation(fields);
  const chosen = m365Mailboxes(fields);
  const refused = useCallback(
    (err: unknown) =>
      serverMessage(
        err instanceof SignInRefused
          ? { code: err.code, params: err.params, detail: err.message }
          : null,
        t("microsoft365Fields.connectFailed"),
      ),
    [t],
  );

  // Whether the organisation has connected Microsoft 365 decides what the form offers.
  useEffect(() => {
    let live = true;
    signInStatus(MICROSOFT_365)
      .then((answered) => live && setStatus(answered))
      .catch((err: unknown) => live && setProblem(refused(err)));
    return () => {
      live = false;
    };
  }, [refused]);

  const connect = async () => {
    setProblem("");
    setConnecting(true);
    try {
      const done = await signInSource({
        source_id: sourceId.trim(),
        kind: MICROSOFT_365,
        account: fields.m365_account.trim(),
        scopes: M365_SCOPES,
      });
      setFields({
        ...fields,
        m365_refresh_token: done.refresh_token,
        m365_connected_as: done.account,
      });
    } catch (err) {
      setProblem(refused(err));
    } finally {
      setConnecting(false);
    }
  };

  // Another choice of mailboxes is not the one that was counted.
  const choose = (changed: Record<string, string>) => {
    setFound(null);
    setFields({ ...fields, ...changed });
  };

  const check = async () => {
    if (chosen === null) return;
    setProblem("");
    setFound(null);
    setChecking(true);
    try {
      setFound(await m365CheckMailboxes(chosen));
    } catch (err) {
      setProblem(refused(err));
    } finally {
      setChecking(false);
    }
  };

  const notSetUp = status !== null && !status.configured;
  const notAllowed =
    organisation && status !== null && status.configured && !status.organisation_mailboxes;
  // Where an administrator sets it up, named as the menu names it in this language.
  const where = `${t("navBar.groupAdmin")} › ${t("navBar.itemEmail")}`;
  return (
    <>
      <Select
        label={t("microsoft365Fields.whose")}
        description={t("microsoft365Fields.whoseHelp")}
        data={[
          { value: M365_ONE, label: t("microsoft365Fields.whoseOne") },
          { value: M365_ORGANISATION, label: t("microsoft365Fields.whoseOrganisation") },
        ]}
        value={organisation ? M365_ORGANISATION : M365_ONE}
        onChange={(value) => choose({ m365_whose: value ?? M365_ONE })}
        allowDeselect={false}
        style={WIDE}
        data-testid="microsoft-365-m365_whose"
      />
      {!organisation && (
        <>
          <TextInput
            label={t("microsoft365Fields.account")}
            description={t("microsoft365Fields.accountHelp")}
            placeholder="ada@example.com"
            required
            style={WIDE}
            value={fields.m365_account ?? ""}
            // Another mailbox is not the one that was approved: the approval is asked again.
            onChange={(e) =>
              setFields({
                ...fields,
                m365_account: e.currentTarget.value,
                m365_refresh_token: "",
                m365_connected_as: "",
              })
            }
            data-testid="microsoft-365-m365_account"
          />
          <Checkbox
            label={t("microsoft365Fields.readsMail")}
            description={t("microsoft365Fields.readsMailHelp")}
            checked
            readOnly
            style={WIDE}
            data-testid="microsoft-365-reads-mail"
          />
        </>
      )}
      {organisation && (
        <>
          <Select
            label={t("microsoft365Fields.choice")}
            data={[
              { value: M365_EVERYONE, label: t("microsoft365Fields.choiceEveryone") },
              { value: M365_GROUP, label: t("microsoft365Fields.choiceGroup") },
              { value: M365_LIST, label: t("microsoft365Fields.choiceList") },
            ]}
            value={fields.m365_choice ?? null}
            onChange={(value) => choose({ m365_choice: value ?? "" })}
            required
            allowDeselect={false}
            style={WIDE}
            data-testid="microsoft-365-m365_choice"
          />
          {fields.m365_choice === M365_GROUP && (
            <TextInput
              label={t("microsoft365Fields.group")}
              description={t("microsoft365Fields.groupHelp")}
              placeholder="sales@example.com"
              required
              style={WIDE}
              value={fields.m365_group ?? ""}
              onChange={(e) => choose({ m365_group: e.currentTarget.value })}
              data-testid="microsoft-365-m365_group"
            />
          )}
          {fields.m365_choice === M365_LIST && (
            <Textarea
              label={t("microsoft365Fields.list")}
              description={t("microsoft365Fields.listHelp")}
              placeholder={"ada@example.com\ngrace@example.com"}
              autosize
              minRows={3}
              required
              style={WIDE}
              value={fields.m365_list ?? ""}
              onChange={(e) => choose({ m365_list: e.currentTarget.value })}
              data-testid="microsoft-365-m365_list"
            />
          )}
          <Checkbox
            label={t("microsoft365Fields.readsMail")}
            description={t("microsoft365Fields.readsMailHelp")}
            checked
            readOnly
            style={WIDE}
            data-testid="microsoft-365-reads-mail"
          />
          <Text size="sm" c="dimmed" style={WIDE} data-testid="microsoft-365-every-mailbox">
            {t("microsoft365Fields.everyMailbox")}
          </Text>
        </>
      )}
      {notSetUp && (
        <Alert color="yellow" variant="light" style={WIDE} data-testid="microsoft-365-not-set-up">
          {status.may_configure ? (
            <>
              {t("microsoft365Fields.notSetUp")}{" "}
              <Anchor component={Link} to={MAIL_PLATFORMS_ROUTE} data-testid="microsoft-365-set-up">
                {t("microsoft365Fields.setUpLink", { where })}
              </Anchor>
            </>
          ) : (
            t("microsoft365Fields.notSetUpAsk", { where })
          )}
        </Alert>
      )}
      {notAllowed && (
        <Alert color="yellow" variant="light" style={WIDE} data-testid="microsoft-365-not-allowed">
          {status.may_configure ? (
            <>
              {t("microsoft365Fields.notAllowed")}{" "}
              <Anchor component={Link} to={MAIL_PLATFORMS_ROUTE} data-testid="microsoft-365-allow">
                {t("microsoft365Fields.allowLink", { where })}
              </Anchor>
            </>
          ) : (
            t("microsoft365Fields.notAllowedAsk", { where })
          )}
        </Alert>
      )}
      {organisation && !notSetUp && !notAllowed && (
        <Group style={WIDE} gap="sm" align="center">
          <Button
            type="button"
            variant="default"
            onClick={check}
            loading={checking}
            disabled={status === null || chosen === null}
            data-testid="microsoft-365-check"
          >
            {t("microsoft365Fields.check")}
          </Button>
          {found !== null && (
            <Alert color="green" variant="light" py={4} px="sm" data-testid="microsoft-365-found">
              {t("microsoft365Fields.found", { count: found })}
            </Alert>
          )}
        </Group>
      )}
      {!organisation && !notSetUp && (
        <Group style={WIDE} gap="sm" align="center">
          <Button
            type="button"
            onClick={connect}
            loading={connecting}
            disabled={status === null || m365ConnectMissing(fields, sourceId).length > 0}
            data-testid="microsoft-365-connect"
          >
            {t("microsoft365Fields.connect")}
          </Button>
          {fields.m365_connected_as && fields.m365_refresh_token && (
            <Alert
              color="green"
              variant="light"
              py={4}
              px="sm"
              data-testid="microsoft-365-connected"
            >
              {t("microsoft365Fields.connected", { account: fields.m365_connected_as })}
            </Alert>
          )}
        </Group>
      )}
      {problem && (
        <Alert color="red" variant="light" style={WIDE} data-testid="microsoft-365-problem">
          {problem}
        </Alert>
      )}
    </>
  );
}
