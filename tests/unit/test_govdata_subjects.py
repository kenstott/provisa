# Copyright (c) 2026 Kenneth Stott
# Canary: 2b6e4a19-8f3d-4c72-a915-6e1d3b7f9a02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1798: keyword search over GovData's static schema/table catalog snapshot."""

from provisa.core.models import GovDataSubject
from provisa.govdata.subjects import search_catalog, search_subjects


class TestSearchCatalog:
    def test_matches_a_real_table_not_a_guessed_synonym(self):
        # "inflation" isn't in the ECONOMY subject's name or schema list ("econ") — this only
        # works because the static catalog carries the real econ-schema.yaml table descriptions
        # (e.g. metro_cpi_inflation), not a hand-written keyword map.
        hits = search_catalog("inflation")
        assert any(h["subject"] == "ECONOMY" for h in hits)
        econ_hit = next(h for h in hits if h["schema"] == "econ")
        assert any("cpi" in t.lower() or "inflation" in t.lower() for t in econ_hit["tables"])

    def test_no_match_returns_empty(self):
        assert search_catalog("xyzzy_no_such_topic_zzqq") == []

    def test_empty_query_returns_empty(self):
        assert search_catalog("") == []

    def test_results_ranked_by_score_descending(self):
        hits = search_catalog("inflation")
        scores = [h["score"] for h in hits]
        assert scores == sorted(scores, reverse=True)


class TestSearchSubjects:
    def test_returns_subjects_not_schemas(self):
        subjects = search_subjects("inflation")
        assert GovDataSubject.economy in subjects

    def test_deduplicates_subjects_covering_multiple_matching_schemas(self):
        subjects = search_subjects("inflation")
        assert len(subjects) == len(set(subjects))
