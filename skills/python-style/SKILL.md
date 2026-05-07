---
name: python-style
description: Python coding conventions for this project. Auto-triggers when writing or reviewing Python code.
---

# Python Style Conventions

## File Header
Every `.py` file starts with:
```python
# Copyright (c) 2025 Kenneth Stott
# Canary: <generate unique uuid4>
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations
```

Replace `<generate unique uuid4>` with a freshly generated UUID4 value. Every file gets a unique UUID.

## Import Order
1. `from __future__ import annotations`
2. stdlib (blank line)
3. third-party (blank line)
4. local (blank line)

## Type Hints
- Modern style: `list[str]`, `dict[str, int]`, `X | None` (not `List`, `Optional`)
- Return types required on all functions
- Avoid `Any` — use specific types or generics

## Naming
- `PascalCase` — classes
- `snake_case` — functions, variables, modules
- `_leading_underscore` — private
- `UPPER_SNAKE` — constants

## Formatting
- Line length: 100 (ruff + black)
- Ruff rules: E, F, I, B, UP, ANN, S, A, C4, T20, PT, PTH, SIM, ARG
- Target: Python 3.12+

## Docstrings
Google format:
```python
def func(x: int, y: str) -> bool:
    """Short summary.

    Args:
        x: Description.
        y: Description.

    Returns:
        Description.

    Raises:
        ValueError: When x < 0.
    """
```

## Logging
- No `print()` in production code
- Use `logger = logging.getLogger(__name__)` at module scope
- Debug context tags: `[PARALLEL]`, `[COMPLEXITY]`, `[DYNAMIC_CONTEXT]`

## File Size
- **Max 1000 lines per file** — split by separation of concerns when approaching this limit
- Group related functionality into focused modules

## General
- Dataclasses/Pydantic for data structures (not plain dicts)
- Context managers for resource cleanup
- `pathlib.Path` over `os.path`
- No mutable default arguments
- No bare `except:` — always specify exception type
