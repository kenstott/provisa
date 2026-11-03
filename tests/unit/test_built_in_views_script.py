# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1.
"""The built-in views go to a new schema as one script (_make_built_in_views)."""

from provisa.api.startup_seed import views_script


def test_a_view_ending_in_a_comment_keeps_its_terminator():
    script = views_script(["CREATE VIEW a AS SELECT 1  -- a note", "CREATE VIEW b AS SELECT 2;"])
    assert script == "CREATE VIEW a AS SELECT 1  -- a note\n;\nCREATE VIEW b AS SELECT 2\n;"
