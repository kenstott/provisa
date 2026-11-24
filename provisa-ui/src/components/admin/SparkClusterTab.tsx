// Copyright (c) 2026 Kenneth Stott
// Canary: 4f9c2e83-1d57-4b6a-a0f3-92c7e5b8d164
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225: the organisation's default Spark cluster, set by its administrator: a Spark Connect
// address and the token as a secret reference, with a connection test. A Spark source's own
// cluster is used when it has one; this default where it names none. A demo or local install
// runs on its local test cluster until a default is set.

import { useCallback, useEffect, useState } from "react";
import { Alert, Button, Card, Stack, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  deleteSparkCluster,
  fetchSparkCluster,
  saveSparkCluster,
  testSparkCluster,
  type SparkClusterInUse,
} from "../../api/sparkCluster";
import { useAuth } from "../../context/AuthContext";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

function InUse({ inUse }: { inUse: SparkClusterInUse }) {
  const { t } = useTranslation();
  const text =
    inUse.origin === "organisation"
      ? t("sparkCluster.inUseOrg", { target: inUse.target })
      : inUse.origin === "local_test"
        ? t("sparkCluster.inUseLocal", { target: inUse.target })
        : t("sparkCluster.inUseNone");
  return (
    <Alert
      color={inUse.origin === null ? "yellow" : "blue"}
      variant="light"
      data-testid="spark-cluster-in-use"
    >
      {text}
    </Alert>
  );
}

export function SparkClusterTab() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [address, setAddress] = useState("");
  const [credential, setCredential] = useState("");
  const [kept, setKept] = useState(false);
  const [inUse, setInUse] = useState<SparkClusterInUse | null>(null);
  const [busy, setBusy] = useState("");
  const [said, setSaid] = useState<{ text: string; bad: boolean } | null>(null);
  const [error, setError] = useState("");

  const load = useCallback(() => {
    if (!activeOrgId) return;
    fetchSparkCluster(activeOrgId)
      .then((state) => {
        setAddress(state.cluster?.address ?? "");
        setCredential(state.cluster?.credential ?? "");
        setKept(state.cluster !== null);
        setInUse(state.in_use);
      })
      .catch((e: unknown) => setError(String(e)));
  }, [activeOrgId]);
  useEffect(load, [load]);

  if (error) {
    return (
      <Alert color="red" variant="light" data-testid="spark-cluster-error">
        {error}
      </Alert>
    );
  }
  if (!inUse || !activeOrgId) return null;

  const run = async (what: string, act: () => Promise<string>) => {
    setBusy(what);
    setSaid(null);
    try {
      setSaid({ text: await act(), bad: false });
    } catch (e) {
      setSaid({ text: e instanceof Error ? e.message : String(e), bad: true });
    } finally {
      setBusy("");
    }
  };

  const body = () => ({ address: address.trim(), credential: credential.trim() || null });

  const save = () =>
    run("save", async () => {
      await saveSparkCluster(activeOrgId, body());
      load();
      return t("sparkCluster.saved");
    });

  const remove = () =>
    run("remove", async () => {
      await deleteSparkCluster(activeOrgId);
      load();
      return t("sparkCluster.removed");
    });

  const test = () =>
    run("test", async () => {
      const found = await testSparkCluster(activeOrgId, body());
      return t("sparkCluster.tested", { target: found.target, version: found.spark_version });
    });

  return (
    <Stack gap="md" data-testid="spark-cluster">
      <Text size="sm">{t("sparkCluster.help")}</Text>
      <InUse inUse={inUse} />
      <Card withBorder>
        <FormLayout>
          <FormField size="long" help={t("sparkCluster.addressHelp")}>
            <TextInput
              label={t("sparkCluster.address")}
              placeholder="sc://spark.example.com:15002"
              required
              value={address}
              onChange={(e) => setAddress(e.currentTarget.value)}
              autoComplete="off"
              data-testid="spark-cluster-address"
            />
          </FormField>
          <FormField size="long" help={t("sparkCluster.credentialHelp")}>
            <TextInput
              label={t("sparkCluster.credential")}
              placeholder="${secret:spark_token}"
              value={credential}
              onChange={(e) => setCredential(e.currentTarget.value)}
              autoComplete="off"
              data-testid="spark-cluster-credential"
            />
          </FormField>
          <FormFooter
            onSave={save}
            saving={busy === "save"}
            saveProps={{ "data-testid": "spark-cluster-save", disabled: !address.trim() }}
          >
            <Button
              variant="default"
              onClick={test}
              loading={busy === "test"}
              disabled={busy !== "" && busy !== "test"}
              data-testid="spark-cluster-test"
            >
              {t("sparkCluster.test")}
            </Button>
            {kept && (
              <Button
                variant="default"
                color="red"
                onClick={remove}
                disabled={busy !== ""}
                data-testid="spark-cluster-remove"
              >
                {t("sparkCluster.remove")}
              </Button>
            )}
            {said && (
              <Text size="sm" c={said.bad ? "red" : "green"} data-testid="spark-cluster-said">
                {said.text}
              </Text>
            )}
          </FormFooter>
        </FormLayout>
      </Card>
    </Stack>
  );
}
