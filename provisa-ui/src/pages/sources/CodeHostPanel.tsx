// Copyright (c) 2026 Kenneth Stott
// Canary: 7f8b0ed9-8332-4df4-b270-580d84b60730
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a saved code source's mode -- dev, its code on this server, or built, its image on a
// remote host -- and what moves it between them: build its image (pushed to one of the
// organisation's container registries, or downloaded), the compose service to run it, and the
// remote host's address and one-time pairing code to connect it; once built, whether its host
// answers, re-pinning to the code it reports, and going back to dev.

import { useEffect, useState } from "react";
import {
  Alert,
  Button,
  Code,
  CopyButton,
  Group,
  PasswordInput,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  archiveUrl,
  backToDev,
  connectHost,
  fetchBuild,
  fetchBuilds,
  fetchHostStatus,
  repin,
  startBuild,
  type Arch,
  type Build,
  type HostStatus,
} from "../../api/codeHosts";
import { fetchContainerRegistries, type ContainerRegistry } from "../../api/containerRegistries";
import { FieldGroup, FormField, FormFold, FormFolds } from "../../components/form/FormLayout";

const DOWNLOAD = "download";
const POLL_MS = 2000;

interface Props {
  orgId: string;
  sourceId: string;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

export function CodeHostPanel({ orgId, sourceId, fields, setFields }: Props) {
  const { t } = useTranslation();
  const built = fields.code_mode === "built";
  const [registries, setRegistries] = useState<ContainerRegistry[]>([]);
  const [arch, setArch] = useState<Arch>("amd64");
  const [destination, setDestination] = useState<string>(DOWNLOAD);
  const [build, setBuild] = useState<Build | null>(null);
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [pairing, setPairing] = useState("");
  const [status, setStatus] = useState<HostStatus | null>(null);
  const [busy, setBusy] = useState("");
  const [failed, setFailed] = useState("");

  const say = (e: unknown) => setFailed(String(e instanceof Error ? e.message : e));

  useEffect(() => {
    // Read only by whoever may enter the registries: without that right a build is downloaded.
    fetchContainerRegistries(orgId)
      .then((listed) => {
        setRegistries(listed);
        const preferred = listed.find((r) => r.default);
        if (preferred) setDestination(preferred.id);
      })
      .catch(() => setRegistries([]));
    if (!built) {
      fetchBuilds(orgId, sourceId)
        .then((listed) => setBuild(listed[0] ?? null))
        .catch(say);
    }
  }, [orgId, sourceId, built]);

  // A build that is still building is read again until it ends.
  useEffect(() => {
    if (build?.status !== "building") return;
    const id = build.id;
    const timer = setTimeout(() => {
      fetchBuild(orgId, sourceId, id).then(setBuild).catch(say);
    }, POLL_MS);
    return () => clearTimeout(timer);
  }, [build, orgId, sourceId]);

  // A built source's host, asked on opening and again on "Check again".
  const [asked, setAsked] = useState(0);
  useEffect(() => {
    if (!built) return;
    let live = true;
    fetchHostStatus(orgId, sourceId)
      .then((answered) => live && setStatus(answered))
      .catch((e: unknown) => live && say(e));
    return () => {
      live = false;
    };
  }, [orgId, sourceId, built, asked]);
  const loadStatus = () => {
    setStatus(null);
    setAsked((n) => n + 1);
  };

  const run = async (key: string, act: () => Promise<void>) => {
    setBusy(key);
    setFailed("");
    try {
      await act();
    } catch (e) {
      say(e);
    } finally {
      setBusy("");
    }
  };

  const startImage = () =>
    run("build", async () => {
      const download = destination === DOWNLOAD;
      setBuild(
        await startBuild(orgId, sourceId, {
          arch,
          registry_id: download ? null : destination,
          download,
        }),
      );
    });

  const connect = () =>
    run("connect", async () => {
      const answered = await connectHost(orgId, sourceId, {
        host: host.trim(),
        port: Number(port),
        pairing_code: pairing.trim(),
      });
      setPairing("");
      setFields({
        ...fields,
        code_mode: "built",
        code_remote: JSON.stringify(answered.remote),
        code_content_hash: answered.content_hash,
      });
    });

  const toDev = () =>
    run("dev", async () => {
      await backToDev(orgId, sourceId);
      setStatus(null);
      setFields({ ...fields, code_mode: "dev", code_remote: "" });
    });

  const repinTo = (hash: string) =>
    run("repin", async () => {
      const answered = await repin(orgId, sourceId, hash);
      setFields({ ...fields, code_content_hash: answered.content_hash });
      loadStatus();
    });

  const remote = built
    ? (JSON.parse(fields.code_remote || "{}") as { host?: string; port?: number })
    : {};
  const otherCode =
    status?.answering && status.code_hash && status.code_hash !== fields.code_content_hash
      ? status.code_hash
      : null;

  return (
    <FormFolds id="code-host">
      <FormFold
        value="remote-host"
        title={t("codeHost.title")}
        note={
          built
            ? t("codeHost.builtNote", { host: `${remote.host}:${remote.port}` })
            : t("codeHost.devNote")
        }
        defaultOpen
      >
        {failed && (
          <Alert color="red" variant="light" data-testid="code-host-error">
            {failed}
          </Alert>
        )}
        {!built && (
          <>
            <FieldGroup>
              <FormField size="medium">
                <Select
                  label={t("codeHost.arch")}
                  data={[
                    { value: "amd64", label: t("codeHost.arch_amd64") },
                    { value: "arm64", label: t("codeHost.arch_arm64") },
                  ]}
                  value={arch}
                  allowDeselect={false}
                  onChange={(v) => v && setArch(v as Arch)}
                  data-testid="code-host-arch"
                />
              </FormField>
              <FormField size="medium" help={t("codeHost.destinationHelp")}>
                <Select
                  label={t("codeHost.destination")}
                  data={[
                    ...registries.map((r) => ({ value: r.id, label: r.address })),
                    { value: DOWNLOAD, label: t("codeHost.download") },
                  ]}
                  value={destination}
                  allowDeselect={false}
                  onChange={(v) => v && setDestination(v)}
                  data-testid="code-host-destination"
                />
              </FormField>
            </FieldGroup>
            <Group>
              <Button
                onClick={startImage}
                loading={busy === "build" || build?.status === "building"}
                data-testid="code-host-build"
              >
                {t("codeHost.build")}
              </Button>
            </Group>
            {build && <BuildResult orgId={orgId} sourceId={sourceId} build={build} />}
            <Text size="sm" fw={600} mt="md">
              {t("codeHost.connectTitle")}
            </Text>
            <FieldGroup>
              <FormField size="medium" help={t("codeHost.hostHelp")}>
                <TextInput
                  label={t("codeHost.host")}
                  placeholder="gpu-box.example.com"
                  value={host}
                  onChange={(e) => setHost(e.currentTarget.value)}
                  data-testid="code-host-host"
                />
              </FormField>
              <FormField size="short" help={t("codeHost.portHelp")}>
                <TextInput
                  label={t("codeHost.port")}
                  placeholder="7443"
                  inputMode="numeric"
                  value={port}
                  onChange={(e) => setPort(e.currentTarget.value.replace(/\D/g, ""))}
                  data-testid="code-host-port"
                />
              </FormField>
              <FormField size="long" help={t("codeHost.pairingHelp")}>
                <PasswordInput
                  label={t("codeHost.pairing")}
                  value={pairing}
                  onChange={(e) => setPairing(e.currentTarget.value)}
                  autoComplete="off"
                  data-testid="code-host-pairing"
                />
              </FormField>
            </FieldGroup>
            <Group>
              <Button
                variant="default"
                onClick={connect}
                loading={busy === "connect"}
                disabled={!host.trim() || !port || !pairing.trim()}
                data-testid="code-host-connect"
              >
                {t("codeHost.connect")}
              </Button>
            </Group>
          </>
        )}
        {built && (
          <Stack gap="sm">
            <Text size="sm" data-testid="code-host-status">
              {status === null
                ? t("codeHost.statusUnknown")
                : status.answering
                  ? t("codeHost.answering")
                  : t("codeHost.notAnswering", { error: status.error ?? "" })}
            </Text>
            {otherCode && (
              <Alert color="yellow" variant="light" data-testid="code-host-other-code">
                <Stack gap="xs">
                  <Text size="sm">
                    {t("codeHost.otherCode", {
                      pinned: fields.code_content_hash.slice(0, 12),
                      reported: otherCode.slice(0, 12),
                    })}
                  </Text>
                  <Group>
                    <Button
                      size="xs"
                      onClick={() => repinTo(otherCode)}
                      loading={busy === "repin"}
                      data-testid="code-host-repin"
                    >
                      {t("codeHost.repin")}
                    </Button>
                  </Group>
                </Stack>
              </Alert>
            )}
            <Group>
              <Button
                variant="default"
                onClick={loadStatus}
                loading={status === null}
                data-testid="code-host-refresh"
              >
                {t("codeHost.refresh")}
              </Button>
              <Button
                variant="default"
                color="red"
                onClick={toDev}
                loading={busy === "dev"}
                data-testid="code-host-to-dev"
              >
                {t("codeHost.toDev")}
              </Button>
            </Group>
          </Stack>
        )}
      </FormFold>
    </FormFolds>
  );
}

function BuildResult({
  orgId,
  sourceId,
  build,
}: {
  orgId: string;
  sourceId: string;
  build: Build;
}) {
  const { t } = useTranslation();
  if (build.status === "building") {
    return (
      <Text size="sm" c="dimmed" data-testid="code-host-building">
        {t("codeHost.building", { arch: build.arch })}
      </Text>
    );
  }
  if (build.status === "failed") {
    return (
      <Alert color="red" variant="light" data-testid="code-host-build-failed">
        {t("codeHost.buildFailed", { error: build.error ?? "" })}
      </Alert>
    );
  }
  return (
    <Stack gap="xs" data-testid="code-host-built">
      <Text size="sm">
        {build.registry_id
          ? t("codeHost.pushed", { image: build.image_ref })
          : t("codeHost.archived", { image: build.image_ref })}
      </Text>
      {!build.registry_id && (
        <Group>
          <Button
            component="a"
            variant="default"
            size="xs"
            href={archiveUrl(orgId, sourceId, build.id)}
            data-testid="code-host-download"
          >
            {t("codeHost.downloadArchive")}
          </Button>
          <Text size="xs" c="dimmed">
            {t("codeHost.dockerLoad")}
          </Text>
        </Group>
      )}
      {build.compose && (
        <>
          <Group justify="space-between">
            <Text size="sm">{t("codeHost.composeTitle")}</Text>
            <CopyButton value={build.compose}>
              {({ copied, copy }) => (
                <Button
                  size="xs"
                  variant="subtle"
                  onClick={copy}
                  data-testid="code-host-copy-compose"
                >
                  {copied ? t("codeHost.copied") : t("codeHost.copy")}
                </Button>
              )}
            </CopyButton>
          </Group>
          <Code block data-testid="code-host-compose">
            {build.compose}
          </Code>
          <Text size="xs" c="dimmed">
            {t("codeHost.composeHelp")}
          </Text>
        </>
      )}
    </Stack>
  );
}
