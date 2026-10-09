# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3c5a10-2f74-4e9b-b6a1-73c0e4d9f258
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read the deployment cannot answer now, refused by name.

Some reads are refused not because the caller may not make them, and not because the statement
is wrong, but because of the state the deployment is in: a table is kept in a region that cannot
be reached (REQ-1922), or the replica a read needs could not be built (REQ-1661). The statement
is good and the server has not failed; an operator has something to fix, and the refusal says
what and on which table.

Every such refusal is a :class:`ReadRefused`. The pipeline raises it where the read is planned or
run; a surface lets it through like any other refusal, and each transport answers the family in
one place: HTTP 503 with the refusal's ``code`` and ``params``, gRPC ``UNAVAILABLE``, and the
message itself on the transports that carry only a message. A new member needs no surface to
learn of it.
"""

from __future__ import annotations


class ReadRefused(RuntimeError):
    """A read refused because of the deployment's state. ``code`` is the stable identifier of
    the refusal and ``params`` what it is about (the table, and what is wrong with it)."""

    code: str = "query.read_refused"
    params: dict[str, str]
