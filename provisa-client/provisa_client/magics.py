# Copyright (c) 2026 Kenneth Stott
# Canary: f506e966-94cd-4eb7-9dff-696ae03fad1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""IPython cell magics that read from Provisa: ``%load_ext provisa_client.magics``.

    %%sql [name] [--role ROLE]
    SELECT ...

``%%graphql`` and ``%%cypher`` take the same arguments. The cell is answered by
:func:`provisa_client.query` as a pandas DataFrame, which is the cell's result; ``name`` also
binds it in the notebook's namespace. The server detects the statement's language, so the magic's
name says what the cell holds and changes nothing about how it is read.
"""

from __future__ import annotations

from typing import Any

from IPython.core.magic import Magics, cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from provisa_client.reader import query


@magics_class
class ProvisaMagics(Magics):
    def _read(self, magic: Any, line: str, cell: str):
        args = parse_argstring(magic, line)
        df = query(cell, role=args.role)
        if args.name:
            self.shell.user_ns[args.name] = df  # pyright: ignore[reportOptionalMemberAccess]
        return df

    @magic_arguments()
    @argument("name", nargs="?", help="the variable the DataFrame is also bound to")
    @argument("--role", help="the role to act as, in place of the connection's")
    @cell_magic
    def sql(self, line: str, cell: str):
        """A SQL statement, read from Provisa as a DataFrame."""
        return self._read(self.sql, line, cell)

    @magic_arguments()
    @argument("name", nargs="?", help="the variable the DataFrame is also bound to")
    @argument("--role", help="the role to act as, in place of the connection's")
    @cell_magic
    def graphql(self, line: str, cell: str):
        """A GraphQL query, read from Provisa as a DataFrame (its one root field's rows)."""
        return self._read(self.graphql, line, cell)

    @magic_arguments()
    @argument("name", nargs="?", help="the variable the DataFrame is also bound to")
    @argument("--role", help="the role to act as, in place of the connection's")
    @cell_magic
    def cypher(self, line: str, cell: str):
        """A Cypher query, read from Provisa as a DataFrame."""
        return self._read(self.cypher, line, cell)


def load_ipython_extension(ipython: Any) -> None:
    ipython.register_magics(ProvisaMagics)
