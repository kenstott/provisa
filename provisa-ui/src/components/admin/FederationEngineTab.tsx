// Copyright (c) 2026 Kenneth Stott
// Canary: 0ffe6754-b504-44a6-8d12-a6d5e87e8445
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Checkbox,
  NumberInput,
  PasswordInput,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import {
  fetchFederationEngine,
  setFederationEngine,
  type FederationEngineState,
} from "../../api/admin";
import { FederationSettingsCards } from "./settingsCards";
import { PlatformRequired } from "./PlatformRequired";
import { isForbidden } from "./isForbidden";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

// REQ-916: select + configure the federation engine. Changes persist to the platform config and
// take effect on the next service restart (the engine is bound once at boot).
export function FederationEngineTab() {
  const { t } = useTranslation();
  const [state, setState] = useState<FederationEngineState | null>(null);
  const [selected, setSelected] = useState<string>("");
  // String for text/number fields; boolean for checkbox fields.
  const [values, setValues] = useState<Record<string, string | boolean>>({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [forbidden, setForbidden] = useState(false);

  useEffect(() => {
    fetchFederationEngine()
      .then((s) => {
        setState(s);
        // The persisted selection, not the running one: this Select edits what a restart will use.
        setSelected(s.persisted);
        // Seed every key the server RETURNED, keeping booleans as booleans. A secret field is not
        // among them (REQ-1575) and so seeds empty, which is what keeps a save from writing a
        // blank over the stored credential — see `save`.
        const seeded: Record<string, string | boolean> = {};
        for (const key of Object.keys(s.config)) {
          const v = s.config[key];
          seeded[key] = typeof v === "boolean" ? v : v == null ? "" : String(v);
        }
        setValues(seeded);
      })
      .catch((e) => (isForbidden(e) ? setForbidden(true) : setError(String(e))));
  }, []);

  const currentEngine = useMemo(
    () => state?.engines.find((e) => e.key === selected),
    [state, selected],
  );

  // A stored secret counts as filled: the server does not send it back, so an empty box means
  // "unchanged", not "missing" (REQ-1575).
  const incomplete = useMemo(
    () =>
      (currentEngine?.config_fields ?? []).some((f) => {
        if (!f.required) return false;
        if (String(values[f.config_key] ?? "").trim()) return false;
        return !(f.secret && state?.secret_set?.[f.config_key]);
      }),
    [currentEngine, values, state],
  );

  const save = async () => {
    if (!state) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const body: { engine: string } & Record<string, unknown> = { engine: selected };
      for (const f of currentEngine?.config_fields ?? []) {
        const v = values[f.config_key];
        if (f.type === "boolean") {
          body[f.config_key] = v === true;
          continue;
        }
        const raw = String(v ?? "").trim();
        // A secret field is only sent when it was typed into: absent means "keep what is stored",
        // and the box starts empty because the server never sent the value back (REQ-1575).
        if (f.secret && raw === "" && state.secret_set?.[f.config_key]) continue;
        // Send blanks otherwise, so clearing a field resets it server-side.
        body[f.config_key] = f.type === "number" && raw !== "" ? Number(raw) : raw;
      }
      const res = await setFederationEngine(body);
      setMsg(
        res.restart_required
          ? t("federationEngineTab.savedRestartRequired")
          : t("federationEngineTab.saved"),
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (forbidden) return <PlatformRequired />;
  if (error && !state) return <Alert color="red">{error}</Alert>;
  if (!state) return <Text>{t("federationEngineTab.loading")}</Text>;

  const fields = currentEngine?.config_fields ?? [];
  const engineOptions = state.engines.map((e) => ({
    value: e.key,
    label: `${e.label}${e.managed ? ` (${e.managed})` : ""}${
      e.key === state.current ? t("federationEngineTab.current") : ""
    }${e.unavailable ? t("federationEngineTab.unavailable", { reason: e.unavailable }) : ""}`,
    disabled: Boolean(e.unavailable),
  }));

  return (
    <Stack gap="md">
      <Text c="dimmed" size="sm">
        {t("federationEngineTab.intro")}
      </Text>

      <FormLayout>
        <FormField size="medium">
          <Select
            label={t("federationEngineTab.engineLabel")}
            data={engineOptions}
            value={selected}
            onChange={(v) => {
              if (!v) return;
              setSelected(v);
              setMsg("");
            }}
            allowDeselect={false}
            data-testid="federation-engine-select"
          />
        </FormField>
        {currentEngine && (
          <FormField size="full">
            <Text c="dimmed" size="xs">
              {currentEngine.description}
            </Text>
          </FormField>
        )}

        {fields
          .filter((f) => f.type !== "boolean")
          .map((f) =>
            f.type === "select" ? (
              <FormField size="medium" key={f.config_key}>
                <Select
                  label={f.label}
                  required={f.required}
                  data={(f.options ?? []).map((o) => ({ value: o.value, label: o.label }))}
                  value={String(values[f.config_key] ?? "")}
                  onChange={(v) => setValues((cur) => ({ ...cur, [f.config_key]: v ?? "" }))}
                />
              </FormField>
            ) : f.type === "number" ? (
              <FormField size="short" key={f.config_key}>
                <NumberInput
                  label={f.label}
                  required={f.required}
                  placeholder={f.placeholder}
                  value={
                    String(values[f.config_key] ?? "") === "" ? "" : Number(values[f.config_key])
                  }
                  onChange={(v) =>
                    setValues((cur) => ({ ...cur, [f.config_key]: String(v ?? "") }))
                  }
                />
              </FormField>
            ) : f.secret ? (
              // A secret is typed and shown masked, with the eye to reveal it.
              <FormField size="medium" key={f.config_key}>
                <PasswordInput
                  label={f.label}
                  required={f.required}
                  placeholder={f.placeholder}
                  value={String(values[f.config_key] ?? "")}
                  onChange={(e) => {
                    const next = e.currentTarget.value;
                    setValues((v) => ({ ...v, [f.config_key]: next }));
                  }}
                  autoComplete="new-password"
                />
              </FormField>
            ) : (
              <FormField size="medium" key={f.config_key}>
                <TextInput
                  label={f.label}
                  required={f.required}
                  placeholder={f.placeholder}
                  value={String(values[f.config_key] ?? "")}
                  onChange={(e) => {
                    const next = e.currentTarget.value;
                    setValues((v) => ({ ...v, [f.config_key]: next }));
                  }}
                />
              </FormField>
            ),
          )}
        {fields.some((f) => f.type === "boolean") && (
          <FormChecks>
            {fields
              .filter((f) => f.type === "boolean")
              .map((f) => (
                <FormField size="short" key={f.config_key}>
                  <Checkbox
                    label={f.label}
                    checked={values[f.config_key] === true}
                    onChange={(e) => {
                      const next = e.currentTarget.checked;
                      setValues((v) => ({ ...v, [f.config_key]: next }));
                    }}
                  />
                </FormField>
              ))}
          </FormChecks>
        )}
      </FormLayout>

      {state.env_pinned_engine && (
        <Alert color="orange" variant="light" data-testid="federation-engine-env-pin">
          {t("federationEngineTab.envPinned", { engine: state.env_pinned_engine })}
        </Alert>
      )}

      {state.persisted !== state.current && (
        <Alert color="blue" variant="light" data-testid="federation-engine-pending">
          {t("federationEngineTab.pendingSelection", {
            engine: state.persisted,
            running: state.current,
          })}
        </Alert>
      )}

      <Alert color="yellow" variant="light">
        {state.restart_required_note}
      </Alert>

      <FormFooter
        onSave={save}
        saving={saving}
        saveProps={{ "data-testid": "federation-engine-save-button", disabled: incomplete }}
      >
        {msg && (
          <Text c="green" size="sm">
            {msg}
          </Text>
        )}
        {error && (
          <Text c="red" size="sm">
            {error}
          </Text>
        )}
      </FormFooter>

      {/* REQ-1349: sampling, CDC and remote-GraphQL limits govern how this engine reaches sources,
          so they sit with the engine rather than on the Admin overview. */}
      <FederationSettingsCards />
    </Stack>
  );
}
