# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c0d52-8a17-4e9b-b2d4-71c5e0a9f318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Find terms in a document (REQ-2179): the glossary's scan of a reference library document.

The document is one in the org's reference library (REQ-2185), picked there or added to it as a
proposed document. :mod:`.scan` reads its passages with the org's AI model: the concepts each
passage defines or uses (:func:`.scan.extraction_prompt`), then those mentions resolved into
candidate terms (:func:`.scan.resolution_prompt`), each with its definition, variants, conflicting
explanations and source passages, and matched against the glossary's terms and synonyms.
:mod:`.model` holds every model call to the org's AI settings and the acting role's LLM budget
(REQ-1174). :mod:`.queue` keeps the scan and its candidates as drafts in the review queue the
steward works through, and :mod:`.review` turns an accepted or merged draft into glossary terms.
"""

# Requirements: REQ-2179
