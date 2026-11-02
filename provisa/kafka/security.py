# Copyright (c) 2026 Kenneth Stott
# Canary: b5a7269c-e80f-4a13-b5c6-0b6951750ba4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a Kafka source reaches a secured cluster: TLS and SASL, for Confluent Cloud, MSK and the like.

One value, :class:`KafkaSecurity`, is built from what the source holds and handed to every path that
opens Kafka: a read and a landing (``kafka.fetch``), discovery's sample, the landed offsets, the push
listener, the subscription, a SingleStore pipeline and the Trino catalog. Each asks it for the form
its client takes (:meth:`KafkaSecurity.client_kwargs` for aiokafka,
:meth:`KafkaSecurity.client_properties` for Trino's Java client,
:meth:`KafkaSecurity.pipeline_config` for SingleStore's librdkafka).

A source keeps these in its standard fields: ``username`` and ``password`` as entered (the value, or
a ``${secret:...}`` reference, resolved here, at use) and the rest in ``federation_hints``. A source
naming none of them is a plain cluster, and every client is opened exactly as it always was.

The CA is the certificate text (PEM), not a path: the app, a Trino container and an engine shard's
pod share no directory, and a certificate is no secret. Named, it is the only trust the client has;
unnamed, the client trusts the public CAs, which is what Confluent Cloud and MSK present. Hostname
verification is always on.
"""

# Requirements: REQ-147

from __future__ import annotations

import ssl
from dataclasses import dataclass, field
from typing import Any, Mapping

PROTOCOL_PLAINTEXT = "PLAINTEXT"
PROTOCOL_SSL = "SSL"
PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT"
PROTOCOL_SASL_SSL = "SASL_SSL"
SECURITY_PROTOCOLS = (
    PROTOCOL_PLAINTEXT,
    PROTOCOL_SSL,
    PROTOCOL_SASL_PLAINTEXT,
    PROTOCOL_SASL_SSL,
)
_SASL_PROTOCOLS = frozenset({PROTOCOL_SASL_PLAINTEXT, PROTOCOL_SASL_SSL})
_TLS_PROTOCOLS = frozenset({PROTOCOL_SSL, PROTOCOL_SASL_SSL})

_LOGIN_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
}
SASL_MECHANISMS = tuple(_LOGIN_MODULES)

#: The ``federation_hints`` keys a source keeps its settings under.
HINT_PROTOCOL = "kafka_security_protocol"
HINT_MECHANISM = "kafka_sasl_mechanism"
HINT_CA = "kafka_ssl_ca"

#: A ``kafka_sources[]`` entry's ``auth.type`` -> its SASL mechanism.
_DECLARED_MECHANISMS = {
    "sasl_plain": "PLAIN",
    "sasl_scram_256": "SCRAM-SHA-256",
    "sasl_scram_512": "SCRAM-SHA-512",
}


@dataclass(frozen=True)
class KafkaSecurity:  # REQ-147
    """The settings a client opens a cluster with. The password stays out of the repr, so a value
    that reaches a log or a traceback carries none. Settings that disagree are refused by name:
    nothing here supplies a credential a source did not name."""

    protocol: str = PROTOCOL_PLAINTEXT
    mechanism: str | None = None
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    ca: str | None = None

    def __post_init__(self) -> None:
        if self.protocol not in SECURITY_PROTOCOLS:
            raise ValueError(
                f"Unknown Kafka security protocol {self.protocol!r}: expected one of "
                f"{', '.join(SECURITY_PROTOCOLS)}"
            )
        if self.sasl:
            if self.mechanism not in SASL_MECHANISMS:
                raise ValueError(
                    f"Kafka security protocol {self.protocol} needs a SASL mechanism, one of "
                    f"{', '.join(SASL_MECHANISMS)}; the source names {self.mechanism!r}"
                )
            if not self.username or not self.password:
                raise ValueError(
                    f"Kafka security protocol {self.protocol} needs a username and a password; "
                    "the source is missing at least one"
                )
        elif self.mechanism or self.username or self.password:
            raise ValueError(
                f"Kafka security protocol {self.protocol} carries no SASL settings; the source "
                "names some"
            )
        if self.ca and not self.tls:
            raise ValueError(
                f"Kafka security protocol {self.protocol} has no TLS; the source names a CA "
                "certificate"
            )

    @property
    def sasl(self) -> bool:
        return self.protocol in _SASL_PROTOCOLS

    @property
    def tls(self) -> bool:
        return self.protocol in _TLS_PROTOCOLS

    @classmethod
    def of_settings(
        cls, *, username: str | None, password: str | None, hints: Mapping[str, Any] | None
    ) -> "KafkaSecurity":
        """From what a source holds, its references resolved. A source naming no protocol is a
        plain cluster: the protocol every Kafka source used before it could name one (REQ-147)."""
        from provisa.core.secrets import resolve_secrets

        held = hints or {}
        protocol = held.get(HINT_PROTOCOL) or PROTOCOL_PLAINTEXT
        # A username or password left on a source that speaks no SASL was never read by Kafka.
        sasl = protocol in _SASL_PROTOCOLS
        ca = held.get(HINT_CA)
        return cls(
            protocol=protocol,
            mechanism=held.get(HINT_MECHANISM) or None,
            username=resolve_secrets(username) if sasl and username else None,
            password=resolve_secrets(password) if sasl and password else None,
            ca=resolve_secrets(ca) if ca else None,
        )

    @classmethod
    def of_source(cls, source: Any) -> "KafkaSecurity":
        """From a registered source (a :class:`provisa.core.models.Source`)."""
        return cls.of_settings(
            username=source.username, password=source.password, hints=source.federation_hints
        )

    def client_kwargs(self) -> dict[str, Any]:
        """Keyword arguments for an aiokafka client. A plain cluster's are none."""
        if self.protocol == PROTOCOL_PLAINTEXT:
            return {}
        kwargs: dict[str, Any] = {"security_protocol": self.protocol}
        if self.tls:
            # Hostname and chain verified; the named CA alone, else the public CAs.
            kwargs["ssl_context"] = ssl.create_default_context(cadata=self.ca)
        if self.sasl:
            kwargs["sasl_mechanism"] = self.mechanism
            kwargs["sasl_plain_username"] = self.username
            kwargs["sasl_plain_password"] = self.password
        return kwargs

    def client_properties(self) -> str | None:
        """A Java client's properties file (Trino's ``kafka.config.resources``). None for a plain
        cluster, whose catalog names no such file."""
        if self.protocol == PROTOCOL_PLAINTEXT:
            return None
        lines = [f"security.protocol={self.protocol}"]
        if self.sasl:
            assert self.mechanism and self.username and self.password  # __post_init__
            lines.append(f"sasl.mechanism={self.mechanism}")
            jaas = (
                f"{_LOGIN_MODULES[self.mechanism]} required "
                f'username="{_jaas_quoted(self.username)}" '
                f'password="{_jaas_quoted(self.password)}";'
            )
            # The properties format reads a backslash as an escape of its own.
            lines.append("sasl.jaas.config=" + jaas.replace("\\", "\\\\"))
        if self.ca:
            lines.append("ssl.truststore.type=PEM")
            pem = self.ca.strip().replace("\r\n", "\n")
            lines.append("ssl.truststore.certificates=" + pem.replace("\n", "\\n"))
        return "\n".join(lines)

    def pipeline_config(self) -> tuple[dict[str, str], dict[str, str]] | None:
        """What a SingleStore pipeline's ``CONFIG`` and ``CREDENTIALS`` clauses. None for a
        plain cluster. A pipeline reads a CA only from a file on SingleStore's nodes, so a source
        naming one keeps the relay (``singlestore_pipeline.lands_by_pipeline``)."""
        if self.protocol == PROTOCOL_PLAINTEXT:
            return None
        config: dict[str, str] = {"security.protocol": self.protocol}
        credentials: dict[str, str] = {}
        if self.sasl:
            assert self.mechanism and self.username and self.password  # __post_init__
            config["sasl.mechanism"] = self.mechanism
            config["sasl.username"] = self.username
            credentials["sasl.password"] = self.password
        return config, credentials


def _jaas_quoted(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def check_settings(
    username: str | None, password: str | None, hints: Mapping[str, Any] | None
) -> None:
    """Refuse settings that disagree, as held: references are not resolved."""
    held = hints or {}
    protocol = held.get(HINT_PROTOCOL) or PROTOCOL_PLAINTEXT
    sasl = protocol in _SASL_PROTOCOLS
    KafkaSecurity(
        protocol=protocol,
        mechanism=held.get(HINT_MECHANISM) or None,
        username=(username or None) if sasl else None,
        password=(password or None) if sasl else None,
        ca=held.get(HINT_CA) or None,
    )


#: A plain cluster: what a Kafka client with no source behind it (a CDC transport, a topic output)
#: opens, and what a caller naming no settings means.
PLAINTEXT = KafkaSecurity()


def declared_settings(kafka_source: Mapping[str, Any]) -> dict[str, Any]:
    """A ``kafka_sources[]`` entry's settings in the shape a source holds them (``auth``,
    ``security_protocol``, ``ssl_ca``), references as written. Its ``auth`` alone speaks SASL over
    no TLS, as it always has; ``security_protocol`` names anything else."""
    auth = kafka_source.get("auth") or {}
    mechanism = _DECLARED_MECHANISMS.get(auth.get("type", ""))
    protocol = kafka_source.get("security_protocol") or (
        PROTOCOL_SASL_PLAINTEXT if mechanism else None
    )
    hints = {
        HINT_PROTOCOL: protocol,
        HINT_MECHANISM: mechanism,
        HINT_CA: kafka_source.get("ssl_ca"),
    }
    return {
        "username": auth.get("username", ""),
        "password": auth.get("password", ""),
        "hints": {key: value for key, value in hints.items() if value},
    }


def of_declared(kafka_source: Mapping[str, Any]) -> KafkaSecurity:
    """The settings of a ``kafka_sources[]`` entry, resolved."""
    return KafkaSecurity.of_settings(**declared_settings(kafka_source))
