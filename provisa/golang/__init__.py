# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8d2f6c-1e95-4b37-a0c2-8f3e6d9b5a14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Go source type (REQ-2200): a Go package whose exported functions, tagged ``@table``,
``@command``, ``@stream`` or ``@columns`` in their doc comments, are tables, commands, live
tables and batch column transforms, typed by their signatures. No SDK: the code imports nothing
of Provisa's. See docs/arch/code-sources.md."""
