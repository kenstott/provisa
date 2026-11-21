// Copyright (c) 2026 Kenneth Stott
// Canary: 04a509a9-4498-4bfe-8534-0dcb1f3d73d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { describe, expect, it } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";
import { KafkaFields } from "../KafkaFields";
import { kafkaCredentials, kafkaFieldsFromSource, kafkaHints } from "../kafka";

function Harness({ initial }: { initial: Record<string, string> }) {
  const [fields, setFields] = useState(initial);
  return <KafkaFields fields={fields} setFields={setFields} />;
}

const SECURED = {
  kafka_security_protocol: "SASL_SSL",
  kafka_sasl_mechanism: "SCRAM-SHA-256",
  kafka_ssl_ca: "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----",
  username: "api-key",
  password: "${secret:KAFKA_PW}",
};

describe("what a Kafka source saves", () => {
  it("names nothing for a plain cluster", () => {
    expect(kafkaHints({})).toEqual({});
    expect(kafkaHints({ kafka_security_protocol: "PLAINTEXT" })).toEqual({});
    expect(kafkaCredentials({ kafka_security_protocol: "PLAINTEXT" })).toEqual({
      username: "",
      password: "",
    });
  });

  it("holds the password as entered and comes back as it was saved", () => {
    const saved = {
      federationHintsJson: JSON.stringify(kafkaHints(SECURED)),
      username: kafkaCredentials(SECURED).username,
      passwordRef: kafkaCredentials(SECURED).password,
    };
    expect(saved.passwordRef).toBe("${secret:KAFKA_PW}");
    expect(kafkaFieldsFromSource(saved)).toEqual(SECURED);
  });

  it("drops what a protocol does not use", () => {
    expect(kafkaHints({ ...SECURED, kafka_security_protocol: "SSL" })).toEqual({
      kafka_security_protocol: "SSL",
      kafka_ssl_ca: SECURED.kafka_ssl_ca,
    });
    expect(kafkaHints({ ...SECURED, kafka_security_protocol: "SASL_PLAINTEXT" })).toEqual({
      kafka_security_protocol: "SASL_PLAINTEXT",
      kafka_sasl_mechanism: "SCRAM-SHA-256",
    });
    expect(kafkaCredentials({ ...SECURED, kafka_security_protocol: "SSL" })).toEqual({
      username: "",
      password: "",
    });
  });

  it("invents no mechanism", () => {
    expect(kafkaHints({ kafka_security_protocol: "SASL_SSL" })).toEqual({
      kafka_security_protocol: "SASL_SSL",
    });
  });

  it("reads a source that names no settings as a plain cluster", () => {
    expect(kafkaFieldsFromSource({}).kafka_security_protocol).toBe("PLAINTEXT");
  });
});

describe("the Kafka security fields", () => {
  it("a plain cluster shows the protocol alone", () => {
    renderInForm(<Harness initial={{}} />);
    expect(screen.getByTestId("kafka-security-protocol-select")).toHaveValue("PLAINTEXT");
    expect(screen.queryByTestId("kafka-sasl-mechanism-select")).toBeNull();
    expect(screen.queryByTestId("kafka-username-input")).toBeNull();
    expect(screen.queryByTestId("kafka-password-input")).toBeNull();
    expect(screen.queryByTestId("kafka-ssl-ca-input")).toBeNull();
  });

  it("TLS alone shows the CA and no credential", () => {
    renderInForm(<Harness initial={{ kafka_security_protocol: "SSL" }} />);
    expect(screen.getByTestId("kafka-ssl-ca-input")).toBeInTheDocument();
    expect(screen.queryByTestId("kafka-username-input")).toBeNull();
  });

  it("SASL over no TLS shows the credential and no CA", () => {
    renderInForm(<Harness initial={{ kafka_security_protocol: "SASL_PLAINTEXT" }} />);
    expect(screen.getByTestId("kafka-password-input")).toBeInTheDocument();
    expect(screen.queryByTestId("kafka-ssl-ca-input")).toBeNull();
  });

  it("SASL over TLS shows every field, and what is typed stays", () => {
    renderInForm(<Harness initial={SECURED} />);
    expect(screen.getByTestId("kafka-sasl-mechanism-select")).toHaveValue("SCRAM-SHA-256");
    const password = screen.getByTestId("kafka-password-input");
    expect(password).toHaveValue("${secret:KAFKA_PW}");
    fireEvent.change(password, { target: { value: "${env:KAFKA_PW}" } });
    expect(password).toHaveValue("${env:KAFKA_PW}");
    expect(screen.getByTestId("kafka-ssl-ca-input")).toHaveValue(SECURED.kafka_ssl_ca);
  });
});
