# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2f8b04-1d7a-4e59-9f3b-71a0c4d2e885
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1576: the mail transport is a registry entry, and picking one is fail-closed."""

import pytest

from provisa.core.mail import (
    MailgunEmailSender,
    MailNotConfiguredError,
    Microsoft365EmailSender,
    PostmarkEmailSender,
    ResendEmailSender,
    SendgridEmailSender,
    SesEmailSender,
    SmtpEmailSender,
    email_sender,
)
from provisa.core.mail_registry import (
    MailProviderSpec,
    get_mail_provider_spec,
    mail_provider_registry,
)
from provisa.core.models import MailConfig


class TestTheShippedTransports:
    """The several methods deployments actually use, each reachable by its key."""

    def test_every_popular_method_is_registered(self):
        keys = [s.key for s in mail_provider_registry()]
        assert keys == [
            "smtp",
            "resend",
            "sendgrid",
            "mailgun",
            "postmark",
            "ses",
            "microsoft365",
        ]

    @pytest.mark.parametrize(
        "key,adapter",
        [
            ("smtp", SmtpEmailSender),
            ("resend", ResendEmailSender),
            ("sendgrid", SendgridEmailSender),
            ("mailgun", MailgunEmailSender),
            ("postmark", PostmarkEmailSender),
            ("ses", SesEmailSender),
            ("microsoft365", Microsoft365EmailSender),
        ],
    )
    def test_the_key_selects_that_transport(self, key, adapter):
        spec = get_mail_provider_spec(key)
        assert spec is not None
        if not spec.available():
            pytest.skip(f"{key} needs {spec.requires}")
        assert isinstance(email_sender(MailConfig(provider=key)), adapter)

    def test_each_spec_carries_what_the_form_renders(self):
        for spec in mail_provider_registry():
            assert spec.label and spec.description
            assert spec.config_fields, spec.key
            for f in spec.config_fields:
                assert set(f) == {
                    "config_key",
                    "label",
                    "type",
                    "required",
                    "secret",
                    "placeholder",
                }

    def test_every_field_names_a_real_setting_on_its_config_block(self):
        """A field the form writes must exist on the model, or the value is saved into nothing."""
        cfg = MailConfig()
        for spec in mail_provider_registry():
            block = getattr(cfg, spec.key)
            for f in spec.config_fields:
                assert hasattr(block, f["config_key"]), f"{spec.key}.{f['config_key']}"

    def test_credentials_are_declared_secret(self):
        """REQ-1575 acts on this flag, so a credential that forgets it would be returned by the
        settings GET."""
        secret_fields = {
            (s.key, f["config_key"])
            for s in mail_provider_registry()
            for f in s.config_fields
            if f["secret"]
        }
        assert secret_fields == {
            ("smtp", "password"),
            ("resend", "api_key"),
            ("sendgrid", "api_key"),
            ("mailgun", "api_key"),
            ("postmark", "server_token"),
            ("ses", "secret_access_key"),
            ("microsoft365", "client_secret"),
        }


class TestSelectionIsFailClosed:
    def test_an_unknown_key_names_the_setting(self):
        with pytest.raises(MailNotConfiguredError, match="Unknown mail provider 'sendgird'"):
            email_sender(MailConfig(provider="sendgird"))

    def test_no_provider_at_all_is_refused(self):
        with pytest.raises(MailNotConfiguredError, match="No mail provider is configured"):
            email_sender(MailConfig(provider=""))

    def test_an_uninstalled_sdk_says_what_to_install(self, monkeypatch):
        import dataclasses

        from provisa.core import mail_registry

        spec = get_mail_provider_spec("ses")
        assert spec is not None
        monkeypatch.setitem(
            mail_registry._REGISTRY,
            "ses",
            dataclasses.replace(spec, available=lambda: False),
        )
        with pytest.raises(MailNotConfiguredError, match="install 'boto3'"):
            email_sender(MailConfig(provider="ses"))


class TestRegistration:
    def test_a_deployment_can_register_its_own_transport(self, monkeypatch):
        from provisa.core import mail_registry

        class _Sender:
            def __init__(self, cfg):
                self.cfg = cfg

            def send(self, message):  # pragma: no cover - never sent in this test
                raise AssertionError

        monkeypatch.setitem(
            mail_registry._REGISTRY,
            "housemail",
            MailProviderSpec(
                key="housemail",
                label="House mail",
                description="the deployment's own",
                build=_Sender,
            ),
        )
        assert isinstance(email_sender(MailConfig(provider="housemail")), _Sender)
