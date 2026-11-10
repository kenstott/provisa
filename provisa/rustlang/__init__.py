# Copyright (c) 2026 Kenneth Stott
# Canary: 239cd8ab-5ba1-4ea5-bc1a-9fb92bda9f6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Rust source type (REQ-2227): a Rust library crate whose public functions at its root,
tagged ``@table``, ``@command``, ``@query``, ``@stream``, ``@columns`` or a lifecycle tag in
their doc comments, are tables, commands, live tables, batch column transforms and the
connector's lifecycle, typed by their signatures. No SDK: the code needs nothing of Provisa's;
its row types derive serde's ``Serialize`` and its inputs ``Deserialize``, the way any Rust
program writes JSON. See docs/arch/code-sources.md."""
