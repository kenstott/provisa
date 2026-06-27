# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Graph Explorer Favorites (REQ-781, REQ-782, REQ-783).

TODO: When the favorites model is implemented, replace the inline dataclass
below with the real import, e.g.:
    from provisa.cypher.favorites import GraphExplorerFavorite, FavoritesStore
Model location has not yet been verified — no favorites module found in
provisa/cypher/, provisa/core/, or provisa/api/rest/ as of writing.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Inline model — mirrors the contract the implementation must satisfy.
# Replace with real imports once the model exists.
# ---------------------------------------------------------------------------


@dataclass
class GraphExplorerFavorite:
    """A saved Cypher query with a user-visible label (REQ-781, REQ-782)."""

    id: str
    label: str
    cypher: str


@dataclass
class FavoritesStore:
    """In-memory store for GraphExplorerFavorite entries."""

    _items: dict[str, GraphExplorerFavorite] = field(default_factory=dict)

    def add(self, label: str, cypher: str) -> GraphExplorerFavorite:
        fav_id = str(uuid.uuid4())
        fav = GraphExplorerFavorite(id=fav_id, label=label, cypher=cypher)
        self._items[fav_id] = fav
        return fav

    def get(self, fav_id: str) -> GraphExplorerFavorite | None:
        return self._items.get(fav_id)

    def list(self) -> list[GraphExplorerFavorite]:
        return list(self._items.values())

    def rename(self, fav_id: str, new_label: str) -> GraphExplorerFavorite:
        fav = self._items[fav_id]
        self._items[fav_id] = GraphExplorerFavorite(id=fav.id, label=new_label, cypher=fav.cypher)
        return self._items[fav_id]

    def delete(self, fav_id: str) -> None:
        del self._items[fav_id]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _store_with(*entries: tuple[str, str]) -> FavoritesStore:
    """Return a FavoritesStore pre-populated with (label, cypher) pairs."""
    store = FavoritesStore()
    for label, cypher in entries:
        store.add(label, cypher)
    return store


# ---------------------------------------------------------------------------
# REQ-781: clicking a favorite's label loads its saved Cypher query
# ---------------------------------------------------------------------------


class TestLoadFavoriteCypher:
    def test_load_returns_saved_cypher(self):
        """REQ-781: retrieving a favorite by id returns its saved Cypher string."""
        store = FavoritesStore()
        fav = store.add("Find actors", "MATCH (a:Actor) RETURN a")
        result = store.get(fav.id)
        assert result is not None
        assert result.cypher == "MATCH (a:Actor) RETURN a"

    def test_load_returns_label_unchanged(self):
        """REQ-781: the label is preserved alongside the Cypher query."""
        store = FavoritesStore()
        fav = store.add("My query", "MATCH (n) RETURN n LIMIT 10")
        result = store.get(fav.id)
        assert result is not None
        assert result.label == "My query"

    def test_load_unknown_id_returns_none(self):
        """REQ-781: requesting an unknown id returns None (not an error)."""
        store = FavoritesStore()
        assert store.get("nonexistent-id") is None

    def test_load_one_of_many_returns_correct_cypher(self):
        """REQ-781: correct Cypher is returned when multiple favorites exist."""
        store = FavoritesStore()
        store.add("First", "MATCH (a) RETURN a")
        target = store.add("Second", "MATCH (b:Movie) RETURN b.title")
        store.add("Third", "MATCH (c)-[:ACTED_IN]->(m) RETURN c, m")
        result = store.get(target.id)
        assert result is not None
        assert result.cypher == "MATCH (b:Movie) RETURN b.title"

    def test_multiline_cypher_preserved(self):
        """REQ-781: multi-line Cypher queries are stored and returned verbatim."""
        cypher = "MATCH (p:Person)-[:KNOWS]->(q:Person)\nWHERE p.name = 'Alice'\nRETURN q"
        store = FavoritesStore()
        fav = store.add("Alice's friends", cypher)
        result = store.get(fav.id)
        assert result is not None
        assert result.cypher == cypher


# ---------------------------------------------------------------------------
# REQ-782: inline rename updates label without altering the Cypher query
# ---------------------------------------------------------------------------


class TestRenameFavorite:
    def test_rename_updates_label(self):
        """REQ-782: rename changes the label of the favorite."""
        store = FavoritesStore()
        fav = store.add("Old label", "MATCH (n) RETURN n")
        store.rename(fav.id, "New label")
        updated = store.get(fav.id)
        assert updated is not None
        assert updated.label == "New label"

    def test_rename_does_not_alter_cypher(self):
        """REQ-782: rename leaves the saved Cypher query unchanged."""
        cypher = "MATCH (m:Movie) RETURN m.title ORDER BY m.released"
        store = FavoritesStore()
        fav = store.add("Movies", cypher)
        store.rename(fav.id, "All movies ordered")
        updated = store.get(fav.id)
        assert updated is not None
        assert updated.cypher == cypher

    def test_rename_preserves_id(self):
        """REQ-782: rename does not change the favorite's identity."""
        store = FavoritesStore()
        fav = store.add("Label", "MATCH (n) RETURN n")
        store.rename(fav.id, "New label")
        result = store.get(fav.id)
        assert result is not None
        assert result.id == fav.id

    def test_rename_does_not_affect_other_favorites(self):
        """REQ-782: renaming one favorite leaves all others unchanged."""
        store = FavoritesStore()
        a = store.add("A", "MATCH (a) RETURN a")
        b = store.add("B", "MATCH (b) RETURN b")
        c = store.add("C", "MATCH (c) RETURN c")
        store.rename(b.id, "B-renamed")
        a_result = store.get(a.id)
        c_result = store.get(c.id)
        assert a_result is not None and a_result.label == "A"
        assert c_result is not None and c_result.label == "C"

    def test_rename_to_empty_string_is_allowed(self):
        """REQ-782: the store permits setting an empty label (UI validation is separate)."""
        store = FavoritesStore()
        fav = store.add("Label", "MATCH (n) RETURN n")
        store.rename(fav.id, "")
        result = store.get(fav.id)
        assert result is not None and result.label == ""

    def test_rename_reflected_in_list(self):
        """REQ-782: renamed label appears in the full favorites list."""
        store = FavoritesStore()
        fav = store.add("Old", "MATCH (n) RETURN n")
        store.rename(fav.id, "Updated")
        labels = [f.label for f in store.list()]
        assert "Updated" in labels
        assert "Old" not in labels


# ---------------------------------------------------------------------------
# REQ-783: delete removes the favorite from the list
# ---------------------------------------------------------------------------


class TestDeleteFavorite:
    def test_delete_removes_favorite(self):
        """REQ-783: deleted favorite is no longer retrievable."""
        store = FavoritesStore()
        fav = store.add("To delete", "MATCH (n) RETURN n")
        store.delete(fav.id)
        assert store.get(fav.id) is None

    def test_delete_removes_from_list(self):
        """REQ-783: deleted favorite does not appear in the favorites list."""
        store = FavoritesStore()
        fav = store.add("To delete", "MATCH (n) RETURN n")
        store.delete(fav.id)
        ids = [f.id for f in store.list()]
        assert fav.id not in ids

    def test_delete_does_not_affect_other_favorites(self):
        """REQ-783: deleting one favorite leaves all others intact."""
        store = FavoritesStore()
        a = store.add("A", "MATCH (a) RETURN a")
        b = store.add("B", "MATCH (b) RETURN b")
        c = store.add("C", "MATCH (c) RETURN c")
        store.delete(b.id)
        remaining_ids = {f.id for f in store.list()}
        assert a.id in remaining_ids
        assert c.id in remaining_ids
        assert b.id not in remaining_ids

    def test_delete_reduces_list_length(self):
        """REQ-783: list length decreases by exactly one after a delete."""
        store = _store_with(
            ("Q1", "MATCH (a) RETURN a"),
            ("Q2", "MATCH (b) RETURN b"),
            ("Q3", "MATCH (c) RETURN c"),
        )
        before = len(store.list())
        target = store.list()[1]
        store.delete(target.id)
        assert len(store.list()) == before - 1

    def test_delete_all_leaves_empty_list(self):
        """REQ-783: deleting every favorite results in an empty list."""
        store = _store_with(
            ("Q1", "MATCH (a) RETURN a"),
            ("Q2", "MATCH (b) RETURN b"),
        )
        for fav in list(store.list()):
            store.delete(fav.id)
        assert store.list() == []
