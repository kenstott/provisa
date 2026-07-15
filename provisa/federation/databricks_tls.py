# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""TLS trust resolution for the databricks-sql connector.

The connector's HTTPS transport uses its own bundled certifi CA, not the OS trust store — so
behind a TLS-intercepting proxy (corporate MITM) it rejects the warehouse cert as self-signed,
unlike ODBC/google-auth which read the system keychain. Resolve an explicit CA file (or the
conventional REQUESTS_CA_BUNDLE/SSL_CERT_FILE) so the connector trusts the proxy's root;
DATABRICKS_TLS_NO_VERIFY=1 disables verification for local dev only. Absent all of these, the
connector keeps its default (verified) behavior.
"""

from __future__ import annotations

import os
from typing import Any


def databricks_tls_kwargs() -> dict[str, Any]:
    """The ``dbsql.connect`` TLS kwargs implied by the environment (empty → connector default)."""
    if os.environ.get("DATABRICKS_TLS_NO_VERIFY") == "1":
        return {"_tls_no_verify": True}
    ca_file = (
        os.environ.get("DATABRICKS_TLS_CA_FILE")
        or os.environ.get("REQUESTS_CA_BUNDLE")
        or os.environ.get("SSL_CERT_FILE")
    )
    if ca_file:
        return {"_tls_trusted_ca_file": ca_file}
    return {}
