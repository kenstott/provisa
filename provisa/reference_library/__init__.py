# Copyright (c) 2026 Kenneth Stott
# Canary: 782abb9e-349f-4cb5-b866-194eb778ed55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's reference library (REQ-2185).

Documents an org keeps for its assistants -- policies, regulations, contracts, specifications,
standards -- added as a file or an http(s) URL. A document is read into sections
(:mod:`.extract`), cut into passages (:mod:`.passages`), each passage embedded by the org's
vector model and held in the control plane (:mod:`.store`). :mod:`.library` adds, refreshes,
replaces and removes documents; :mod:`.retrieve` answers a question for a role with the passages
of the documents the role may read, each with what to cite.
"""

# Requirements: REQ-2185
