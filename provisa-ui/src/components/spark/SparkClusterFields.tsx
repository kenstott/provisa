// Copyright (c) 2026 Kenneth Stott
// Canary: 2b9e6d41-c7a8-4f13-8e50-d16a4f7c93b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225 (amended 2026-11-24): a Spark cluster entered as structured fields wherever a cluster
// is set -- a Spark source's own cluster and the organisation's default: host, port (15002 to
// start with), a TLS switch, an optional user id, and the token as a secret reference. Paste
// address splits a pasted sc:// address into the fields and moves a token found in it into a
// secret. The fields compose the one stored address (sparkAddress.ts).

// Requirements: REQ-2225

import { useEffect, useRef, useState } from "react";
import { Alert, Button, Switch, Text, TextInput } from "@mantine/core";
import { ModalLayout } from "../modal/ModalLayout";
import { useTranslation } from "react-i18next";
import { putSecret } from "../../api/secrets";
import { fetchSparkSource, type SparkClusterInUse } from "../../api/sparkCluster";
import { useAuth } from "../../context/AuthContext";
import { FieldGroup, FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import {
  clusterProblems,
  composeAddress,
  NotSparkAddress,
  splitAddress,
  storedFields,
  tokenSecretName,
  type ClusterFields,
} from "./sparkAddress";

interface FieldsProps {
  fields: ClusterFields;
  onChange: (fields: ClusterFields) => void;
  /** Where the fields' test ids start: two clusters may be on one page. */
  testId: string;
  /** A source's own cluster: a blank host means the organisation's default, named here. */
  defaultCluster?: SparkClusterInUse | null;
  readOnly?: boolean;
}

/** The cluster a source leaving its own blank runs on, named. */
function DefaultCluster({ inUse, testId }: { inUse: SparkClusterInUse; testId: string }) {
  const { t } = useTranslation();
  const text =
    inUse.origin === "organisation"
      ? t("sparkCluster.blankUsesOrg", { target: inUse.target })
      : inUse.origin === "local_test"
        ? t("sparkCluster.blankUsesLocal", { target: inUse.target })
        : t("sparkCluster.blankUsesNone", { detail: inUse.detail ?? "" });
  return (
    <FormField size="long">
      <Text
        size="sm"
        c={inUse.origin === null ? "orange" : "dimmed"}
        data-testid={`${testId}-default`}
      >
        {text}
      </Text>
    </FormField>
  );
}

/** Paste address: a pasted sc:// address split into the fields, its token kept as a secret. */
function PasteAddress({
  opened,
  onClose,
  onFields,
  testId,
}: {
  opened: boolean;
  onClose: () => void;
  onFields: (fields: ClusterFields) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [pasted, setPasted] = useState("");
  const [problem, setProblem] = useState("");
  // A token value found in the pasted address, waiting to be kept as a secret.
  const [held, setHeld] = useState<{ fields: ClusterFields; value: string } | null>(null);
  const [secretName, setSecretName] = useState("");
  const [saving, setSaving] = useState(false);
  // REQ-1968 (amended 2026-11-22): the action marks the step's field when empty; it is never
  // disabled by it.
  const required = useRequiredFields<"pasted" | "secretName">(
    held === null ? { pasted } : { secretName },
  );

  const close = () => {
    setPasted("");
    setProblem("");
    setHeld(null);
    setSecretName("");
    onClose();
  };

  const split = () => {
    try {
      const { fields, tokenValue } = splitAddress(pasted);
      if (tokenValue === null) {
        onFields(fields);
        close();
        return;
      }
      setHeld({ fields, value: tokenValue });
      setSecretName(tokenSecretName(fields.host));
      setProblem("");
    } catch (e) {
      if (!(e instanceof NotSparkAddress)) throw e;
      setProblem(t(`sparkCluster.notAddress.${e.reason}`, { part: e.part }));
    }
  };

  const keep = async () => {
    if (!held || !activeOrgId) return;
    setSaving(true);
    try {
      await putSecret(activeOrgId, "org", secretName.trim(), { value: held.value });
      onFields({ ...held.fields, token: `\${secret:${secretName.trim()}}` });
      close();
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <ModalLayout opened={opened} onClose={close} title={t("sparkCluster.pasteTitle")}>
      <FormLayout>
        {held === null ? (
          <FormField size="long" help={t("sparkCluster.pasteHelp")}>
            <TextInput
              label={t("sparkCluster.address")}
              placeholder="sc://spark.example.com:15002/;use_ssl=true"
              {...required.field("pasted")}
              value={pasted}
              onChange={(e) => setPasted(e.currentTarget.value)}
              autoComplete="off"
              data-autofocus
              data-testid={`${testId}-paste-input`}
            />
          </FormField>
        ) : (
          <>
            <FormField size="long">
              <Text size="sm" data-testid={`${testId}-paste-token`}>
                {t("sparkCluster.pasteToken")}
              </Text>
            </FormField>
            <FormField size="medium" help={t("sparkCluster.pasteSecretHelp")}>
              <TextInput
                label={t("sparkCluster.pasteSecretName")}
                {...required.field("secretName")}
                value={secretName}
                onChange={(e) => setSecretName(e.currentTarget.value)}
                autoComplete="off"
                data-testid={`${testId}-paste-secret`}
              />
            </FormField>
          </>
        )}
        <FormFooter onCancel={close} problem={problem || undefined}>
          <Button
            onClick={() => {
              if (!required.validate()) return;
              if (held === null) split();
              else void keep();
            }}
            loading={saving}
            data-testid={`${testId}-paste-apply`}
          >
            {held === null ? t("sparkCluster.pasteApply") : t("sparkCluster.pasteKeep")}
          </Button>
        </FormFooter>
      </FormLayout>
    </ModalLayout>
  );
}

/** The cluster's fields, controlled. */
export function SparkClusterFields({
  fields,
  onChange,
  testId,
  defaultCluster,
  readOnly = false,
}: FieldsProps) {
  const { t } = useTranslation();
  const [pasting, setPasting] = useState(false);
  const wrong = clusterProblems(fields);
  const set = (patch: Partial<ClusterFields>) => onChange({ ...fields, ...patch });
  const tokenGiven = fields.token.trim() !== "";
  return (
    <>
      <FieldGroup>
        <FormField size="medium" help={t("sparkCluster.hostHelp")}>
          <TextInput
            label={t("sparkCluster.host")}
            placeholder="spark.example.com"
            value={fields.host}
            readOnly={readOnly}
            onChange={(e) => set({ host: e.currentTarget.value })}
            error={wrong.host ? t("sparkCluster.badHost") : undefined}
            autoComplete="off"
            data-testid={`${testId}-host`}
          />
        </FormField>
        <FormField size="short">
          <TextInput
            label={t("sparkCluster.port")}
            value={fields.port}
            readOnly={readOnly}
            onChange={(e) => set({ port: e.currentTarget.value })}
            error={wrong.port ? t("sparkCluster.badPort") : undefined}
            inputMode="numeric"
            data-testid={`${testId}-port`}
          />
        </FormField>
        {!readOnly && (
          <FormField size="short" afterLabel>
            <Button
              variant="default"
              onClick={() => setPasting(true)}
              data-testid={`${testId}-paste`}
            >
              {t("sparkCluster.paste")}
            </Button>
          </FormField>
        )}
      </FieldGroup>
      {defaultCluster && !fields.host.trim() && (
        // A line of its own, under the host it stands in for.
        <FieldGroup row>
          <DefaultCluster inUse={defaultCluster} testId={testId} />
        </FieldGroup>
      )}
      <FormChecks>
        <FormField size="short" help={t("sparkCluster.tlsHelp")}>
          <Switch
            label={t("sparkCluster.tls")}
            checked={fields.tls || tokenGiven}
            disabled={readOnly || tokenGiven}
            onChange={(e) => set({ tls: e.currentTarget.checked })}
            data-testid={`${testId}-tls`}
          />
        </FormField>
      </FormChecks>
      <FieldGroup>
        <FormField size="medium" help={t("sparkCluster.userIdHelp")}>
          <TextInput
            label={t("sparkCluster.userId")}
            value={fields.userId}
            readOnly={readOnly}
            onChange={(e) => set({ userId: e.currentTarget.value })}
            autoComplete="off"
            data-testid={`${testId}-user`}
          />
        </FormField>
      </FieldGroup>
      <FormField size="long" help={t("sparkCluster.credentialHelp")}>
        <TextInput
          label={t("sparkCluster.credential")}
          placeholder="${secret:spark_token}"
          value={fields.token}
          readOnly={readOnly}
          onChange={(e) => set({ token: e.currentTarget.value })}
          error={wrong.token ? t("sparkCluster.badToken") : undefined}
          autoComplete="off"
          data-testid={`${testId}-credential`}
        />
      </FormField>
      {fields.extra.length > 0 && (
        <FormField size="long">
          <Text size="sm" c="dimmed" data-testid={`${testId}-extra`}>
            {t("sparkCluster.extra", {
              params: fields.extra.map(([k, v]) => `${k}=${v}`).join("; "),
            })}
          </Text>
        </FormField>
      )}
      <PasteAddress
        opened={pasting}
        onClose={() => setPasting(false)}
        onFields={onChange}
        testId={testId}
      />
    </>
  );
}

/** A Spark source's own cluster, kept as the one address its fields compose (the token a
 * reference in it); blank for the organisation's default. */
export function SparkSourceCluster({
  value,
  onChange,
  readOnly = false,
}: {
  value: string;
  onChange: (address: string) => void;
  readOnly?: boolean;
}) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  // The cluster a source leaving its own blank runs on, named in the form.
  const [defaultCluster, setDefaultCluster] = useState<SparkClusterInUse | null>(null);
  const [defaultFailed, setDefaultFailed] = useState("");
  useEffect(() => {
    if (!activeOrgId) return;
    let live = true;
    fetchSparkSource(activeOrgId)
      .then((found) => live && setDefaultCluster(found.default_cluster))
      .catch((e: unknown) => live && setDefaultFailed(e instanceof Error ? e.message : String(e)));
    return () => {
      live = false;
    };
  }, [activeOrgId]);
  const read = (): { fields: ClusterFields | null; problem: string } => {
    try {
      return { fields: storedFields(value), problem: "" };
    } catch (e) {
      if (!(e instanceof NotSparkAddress)) throw e;
      return { fields: null, problem: t(`sparkCluster.notAddress.${e.reason}`, { part: e.part }) };
    }
  };
  const [state, setState] = useState(read);
  // The address last composed here: a value that differs came from outside (a source loaded).
  const composed = useRef(value);
  useEffect(() => {
    if (value !== composed.current) {
      composed.current = value;
      setState(read());
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read follows value
  }, [value]);

  const change = (fields: ClusterFields) => {
    const address = composeAddress(fields, true);
    composed.current = address;
    setState({ fields, problem: "" });
    onChange(address);
  };

  if (state.fields === null) {
    return (
      <FormField size="long">
        <Alert color="red" variant="light" data-testid="spark-source-cluster-unread">
          {t("sparkCluster.storedUnread", { problem: state.problem })}
        </Alert>
      </FormField>
    );
  }
  return (
    <>
      <SparkClusterFields
        fields={state.fields}
        onChange={change}
        testId="spark-source-cluster"
        defaultCluster={defaultCluster}
        readOnly={readOnly}
      />
      {defaultFailed && !state.fields.host.trim() && (
        <FormField size="long">
          <Alert color="red" variant="light" data-testid="spark-source-cluster-default-failed">
            {defaultFailed}
          </Alert>
        </FormField>
      )}
    </>
  );
}
