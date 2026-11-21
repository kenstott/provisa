# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3c2a91-4d6e-4b8a-9c15-e2a0b7d4f613
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each command a role may use, as its own MCP tool (REQ-2246).

A command visible to the role, where its MCP command binding offers it (REQ-2168), is listed as a
tool named by that binding, its input schema built from the command's arguments. The list is read
from the commands the role sees now (``list_visible_commands``, the source ``list_commands``
projects), so a command registered or retired changes the list at the next tools/list. A call runs
through the one command invocation every surface uses (``invoke_tracked_function``): admitted for
the role, its arguments bound by name, approval, recording and governance of its rows as on SQL,
GraphQL, gRPC and REST.
"""

from __future__ import annotations

from typing import Any

from mcp import types

# The surface language a command's MCP tool is named in.
LANGUAGE = "mcp"


def visible(state: Any, role: str) -> list[dict]:
    """The commands ``role`` sees, each by its MCP command binding."""
    from provisa.api.data.action_exec import list_visible_commands

    return list_visible_commands(state, role, LANGUAGE)


def input_schema(arguments: list[dict]) -> dict[str, Any]:
    """A command's arguments as a tool's input schema: each a property stating its declared type
    (and its JSON type where JSON Schema has one); one that declares no default is required."""
    from provisa.core.argument_defaults import json_type

    properties: dict[str, Any] = {}
    for argument in arguments:
        declared = str(argument["type"])
        prop: dict[str, Any] = {"description": declared}
        kind = json_type(declared)
        if kind is not None:
            prop["type"] = kind
        if "default" in argument:
            prop["default"] = argument["default"]
        properties[argument["name"]] = prop
    required = [a["name"] for a in arguments if "default" not in a]
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


def tool_of(command: dict) -> types.Tool:
    """The MCP tool a visible command is listed as."""
    return types.Tool(
        name=command["name"],
        description=command["description"].strip() or f"The command {command['command']}.",
        inputSchema=input_schema(command["arguments"]),
        # A query command changes nothing; a mutation may.
        annotations=types.ToolAnnotations(readOnlyHint=command["kind"] == "query"),
    )


def tools_for(state: Any, role: str) -> list[types.Tool]:
    """The tools of the commands ``role`` sees."""
    return [tool_of(c) for c in visible(state, role)]


class UnknownTool(LookupError):
    """A tool name the session's list does not offer."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Unknown tool: {name}")


async def call(state: Any, role: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Run the command whose MCP tool is ``name`` as ``role``, with ``arguments`` by name. A name
    no command visible to the role is bound to is refused by name, as an unknown tool."""
    import jsonschema

    from provisa.api.data.action_exec import bind_named_args, invoke_tracked_function

    command = next((c for c in visible(state, role) if c["name"] == name), None)
    if command is None:
        raise UnknownTool(name)
    try:
        jsonschema.validate(instance=arguments, schema=input_schema(command["arguments"]))
    except jsonschema.ValidationError as exc:
        raise ValueError(f"{name}: {exc.message}") from None
    registered = command["command"]
    args = bind_named_args(registered, arguments, state, role)
    return {"rows": await invoke_tracked_function(registered, args, state, role)}
