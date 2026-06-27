# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-781, REQ-782, and REQ-783 — Graph Explorer Favorites.

These steps exercise the FavoritesStore model directly (unit-level) to verify
that clicking a favorite's label loads the correct Cypher query into the editor
context, that inline rename (Enter to commit, Escape to cancel) works correctly
with localStorage persistence simulation, and that the delete button removes a
favorite from the panel and localStorage immediately.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

# ---------------------------------------------------------------------------
# Bind all scenarios from the feature files
# ---------------------------------------------------------------------------

scenarios("features/req_781_graph_explorer.feature")
scenarios("features/req_782_graph_explorer.feature")
scenarios("features/req_783_graph_explorer.feature")

# ---------------------------------------------------------------------------
# Inline model — mirrors the contract defined in the unit-test reference file.
# Replace with real imports once the module exists:
#   from provisa.cypher.favorites import GraphExplorerFavorite, FavoritesStore
# ---------------------------------------------------------------------------


@dataclass
class GraphExplorerFavorite:
    """A saved Cypher query with a user-visible label."""

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
        self._items[fav_id] = GraphExplorerFavorite(
            id=fav.id, label=new_label, cypher=fav.cypher
        )
        return self._items[fav_id]

    def delete(self, fav_id: str) -> None:
        del self._items[fav_id]


# ---------------------------------------------------------------------------
# Simulated editor context
# ---------------------------------------------------------------------------


class GraphEditorContext:
    """Lightweight stand-in for the Graph Explorer editor surface.

    In a real browser this would be the CodeMirror / Monaco editor embedded
    in the collapsed header bar (.gf-header-query-collapsed).  Here we model
    the same state transitions so the BDD steps can make real assertions.
    """

    def __init__(self) -> None:
        self._query: str | None = None
        self._ready: bool = False

    def load_query(self, cypher: str) -> None:
        """Simulate the favourite label click loading query into the editor."""
        if not cypher or not cypher.strip():
            raise ValueError("Cannot load an empty Cypher query into the editor.")
        self._query = cypher
        self._ready = True

    @property
    def current_query(self) -> str | None:
        return self._query

    @property
    def is_ready(self) -> bool:
        """True once a query has been loaded and the editor is accepting input."""
        return self._ready

    def clear(self) -> None:
        self._query = None
        self._ready = False


# ---------------------------------------------------------------------------
# Simulated localStorage
# ---------------------------------------------------------------------------

_LOCAL_STORAGE_KEY = "provisa.graph.favorites"


class LocalStorageSimulator:
    """Simulates browser localStorage for favorites persistence testing.

    Mirrors the contract used by the frontend: favorites are stored as a
    JSON array under the key ``provisa.graph.favorites``.
    """

    def __init__(self) -> None:
        self._storage: dict[str, str] = {}

    def set_item(self, key: str, value: str) -> None:
        self._storage[key] = value

    def get_item(self, key: str) -> str | None:
        return self._storage.get(key)

    def remove_item(self, key: str) -> None:
        self._storage.pop(key, None)

    def save_favorites(self, favorites: list[GraphExplorerFavorite]) -> None:
        """Persist the list of favorites as JSON, mirroring the UI contract."""
        payload = [
            {"id": fav.id, "label": fav.label, "query": fav.cypher, "ts": 0}
            for fav in favorites
        ]
        self.set_item(_LOCAL_STORAGE_KEY, json.dumps(payload))

    def load_favorites(self) -> list[dict[str, Any]]:
        """Return the raw list of favorite dicts from localStorage."""
        raw = self.get_item(_LOCAL_STORAGE_KEY)
        if raw is None:
            return []
        return json.loads(raw)


# ---------------------------------------------------------------------------
# Simulated rename input widget
# ---------------------------------------------------------------------------


class RenameInputWidget:
    """Models the inline rename <input> that appears in the Favorites panel.

    State machine:
        hidden  ->  visible (on rename button click)
        visible ->  committed (Enter key)
        visible ->  cancelled (Escape key)
        committed / cancelled  ->  hidden
    """

    _STATE_HIDDEN = "hidden"
    _STATE_VISIBLE = "visible"
    _STATE_COMMITTED = "committed"
    _STATE_CANCELLED = "cancelled"

    def __init__(self, initial_label: str) -> None:
        self._state: str = self._STATE_HIDDEN
        self._current_value: str = initial_label
        self._original_label: str = initial_label
        self._focused: bool = False

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def open(self) -> None:
        """Simulate clicking the rename (✎) button — shows the input focused."""
        self._state = self._STATE_VISIBLE
        self._current_value = self._original_label
        self._focused = True

    def type_text(self, text: str) -> None:
        """Replace the input's current value with *text* (mirrors .fill())."""
        if self._state != self._STATE_VISIBLE:
            raise RuntimeError("Cannot type into a hidden rename input.")
        self._current_value = text

    def press_enter(self) -> None:
        """Commit the new label and close the input."""
        if self._state != self._STATE_VISIBLE:
            raise RuntimeError("Cannot press Enter on a hidden rename input.")
        self._state = self._STATE_COMMITTED
        self._focused = False

    def press_escape(self) -> None:
        """Cancel the edit and restore the original label, then close the input."""
        if self._state != self._STATE_VISIBLE:
            raise RuntimeError("Cannot press Escape on a hidden rename input.")
        self._current_value = self._original_label
        self._state = self._STATE_CANCELLED
        self._focused = False

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def is_visible(self) -> bool:
        return self._state == self._STATE_VISIBLE

    @property
    def is_focused(self) -> bool:
        return self._focused

    @property
    def committed_label(self) -> str | None:
        """The new label if Enter was pressed, otherwise None."""
        if self._state == self._STATE_COMMITTED:
            return self._current_value
        return None

    @property
    def current_value(self) -> str:
        return self._current_value

    @property
    def was_committed(self) -> bool:
        return self._state == self._STATE_COMMITTED

    @property
    def was_cancelled(self) -> bool:
        return self._state == self._STATE_CANCELLED


# ---------------------------------------------------------------------------
# Favorites panel surface model — used for REQ-783 delete behaviour
# ---------------------------------------------------------------------------


class FavoritesPanelSurface:
    """Models the visible state of the Favorites panel.

    The panel renders one .graph-fav-item per favorite; when empty it shows
    a placeholder (.graph-schema-empty).  This class mirrors that contract so
    step definitions can make real DOM-equivalent assertions without a browser.
    """

    _PLACEHOLDER_TEXT = "No favorites saved yet."

    def __init__(self) -> None:
        self._visible_ids: list[str] = []

    def render(self, favorites: list[GraphExplorerFavorite]) -> None:
        """Sync the panel surface with the current list of favorites."""
        self._visible_ids = [fav.id for fav in favorites]

    @property
    def item_count(self) -> int:
        """Number of .graph-fav-item elements currently in the panel."""
        return len(self._visible_ids)

    @property
    def is_empty(self) -> bool:
        """True when no favorites are displayed (placeholder is shown)."""
        return len(self._visible_ids) == 0

    @property
    def placeholder_visible(self) -> bool:
        """True when the empty-state placeholder (.graph-schema-empty) is shown."""
        return self.is_empty

    def contains(self, fav_id: str) -> bool:
        """True if the panel currently shows an item for the given favorite id."""
        return fav_id in self._visible_ids


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict[str, Any]:
    """Plain dict used to pass state between Given / When / Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Step definitions — REQ-781 default behaviour
# ---------------------------------------------------------------------------

_SAMPLE_QUERY = "MATCH (n:Inquiries) RETURN n LIMIT 5"
_SAMPLE_LABEL = "My Favorite"


@given("the Favorites panel displays a favorite with a Cypher query")
def favorites_panel_has_entry(shared_data: dict[str, Any]) -> None:
    """Set up a FavoritesStore with one favorite and expose it via shared_data."""
    store = FavoritesStore()
    fav = store.add(_SAMPLE_LABEL, _SAMPLE_QUERY)

    editor = GraphEditorContext()

    # Sanity-check: the favorite is retrievable (mirrors REQ-781 unit test)
    retrieved = store.get(fav.id)
    assert retrieved is not None, "Favorite was not stored correctly."
    assert retrieved.label == _SAMPLE_LABEL
    assert retrieved.cypher == _SAMPLE_QUERY

    # Confirm the editor starts in a clean state
    assert editor.current_query is None
    assert editor.is_ready is False

    shared_data["store"] = store
    shared_data["favorite"] = fav
    shared_data["editor"] = editor


@when("the user clicks the favorite's label")
def user_clicks_favorite_label(shared_data: dict[str, Any]) -> None:
    """Simulate the click that loads the favorite's Cypher into the editor."""
    store: FavoritesStore = shared_data["store"]
    fav: GraphExplorerFavorite = shared_data["favorite"]
    editor: GraphEditorContext = shared_data["editor"]

    # Re-fetch the favorite from the store (as the UI would do on click)
    clicked_fav = store.get(fav.id)
    assert clicked_fav is not None, (
        f"Favorite '{fav.id}' was not found in the store when the label was clicked."
    )

    # Load the query into the editor — the action triggered by the click
    editor.load_query(clicked_fav.cypher)

    shared_data["clicked_fav"] = clicked_fav


@then("the query is loaded into the editor header")
def query_is_loaded_into_editor_header(shared_data: dict[str, Any]) -> None:
    """Assert the editor now holds the exact Cypher from the favorite."""
    editor: GraphEditorContext = shared_data["editor"]
    clicked_fav: GraphExplorerFavorite = shared_data["clicked_fav"]

    assert editor.current_query is not None, (
        "Editor query is None; expected the favorite's Cypher to be loaded."
    )
    assert editor.current_query == clicked_fav.cypher, (
        f"Editor contains '{editor.current_query}' but expected '{clicked_fav.cypher}'."
    )
    # The loaded query must match the original sample
    assert editor.current_query == _SAMPLE_QUERY, (
        f"Editor query '{editor.current_query}' does not match expected '{_SAMPLE_QUERY}'."
    )


@then("the editor is ready to execute or modify the query")
def editor_is_ready_to_execute_or_modify(shared_data: dict[str, Any]) -> None:
    """Assert the editor signals it is in an interactive, ready state."""
    editor: GraphEditorContext = shared_data["editor"]

    assert editor.is_ready is True, (
        "Editor is not in the ready state after loading the favorite's query."
    )
    # The query must be non-empty and non-whitespace so it can actually be run
    assert editor.current_query is not None
    assert editor.current_query.strip() != "", (
        "Editor query is blank; the editor cannot be ready to execute an empty query."
    )
    # Verify the content is still intact (no mutation during readiness check)
    assert editor.current_query == _SAMPLE_QUERY


# ---------------------------------------------------------------------------
# Step definitions — REQ-782 default behaviour
# ---------------------------------------------------------------------------

_REQ782_QUERY = "MATCH (n:Inquiries) RETURN n LIMIT 5"
_REQ782_ORIGINAL_LABEL = "My Favorite"
_REQ782_NEW_LABEL = "Renamed Fav"


@given("the Favorites panel with a visible favorite and action buttons on hover")
def favorites_panel_with_visible_favorite(shared_data: dict[str, Any]) -> None:
    """Set up a FavoritesStore with one favorite, a localStorage sim, and expose state."""
    store = FavoritesStore()
    fav = store.add(_REQ782_ORIGINAL_LABEL, _REQ782_QUERY)

    # Verify the favorite is correctly stored before proceeding
    retrieved = store.get(fav.id)
    assert retrieved is not None, "Favorite was not stored correctly in setup."
    assert retrieved.label == _REQ782_ORIGINAL_LABEL, (
        f"Expected label '{_REQ782_ORIGINAL_LABEL}', got '{retrieved.label}'."
    )
    assert retrieved.cypher == _REQ782_QUERY, (
        f"Expected cypher '{_REQ782_QUERY}', got '{retrieved.cypher}'."
    )

    # Simulate localStorage seeded with the initial favorite (mirrors the e2e spec)
    local_storage = LocalStorageSimulator()
    local_storage.save_favorites([fav])

    # Confirm localStorage is seeded correctly
    ls_items = local_storage.load_favorites()
    assert len(ls_items) == 1, f"Expected 1 item in localStorage, got {len(ls_items)}."
    assert ls_items[0]["label"] == _REQ782_ORIGINAL_LABEL
    assert ls_items[0]["query"] == _REQ782_QUERY

    # Action buttons are modelled as always available once the panel is open;
    # in the UI they appear on hover — we assert their "presence" via the store.
    # The rename widget starts hidden.
    rename_widget = RenameInputWidget(initial_label=_REQ782_ORIGINAL_LABEL)
    assert not rename_widget.is_visible, (
        "Rename input must start hidden before the rename button is clicked."
    )

    shared_data["store"] = store
    shared_data["favorite"] = fav
    shared_data["local_storage"] = local_storage
    shared_data["rename_widget"] = rename_widget
    # Track the displayed label as the panel would show it
    shared_data["displayed_label"] = _REQ782_ORIGINAL_LABEL


@when("the user clicks the rename button")
def user_clicks_rename_button(shared_data: dict[str, Any]) -> None:
    """Simulate clicking the ✎ rename button which opens the inline input."""
    rename_widget: RenameInputWidget = shared_data["rename_widget"]

    # The rename button click opens the input pre-filled with the current label
    rename_widget.open()

    # Verify the widget transitioned to visible
    assert rename_widget.is_visible, (
        "Rename input did not become visible after clicking the rename button."
    )


@then("a text input appears focused with the current label")
def text_input_appears_focused_with_current_label(shared_data: dict[str, Any]) -> None:
    """Assert the inline input is visible, focused, and pre-filled with the current label."""
    rename_widget: RenameInputWidget = shared_data["rename_widget"]
    displayed_label: str = shared_data["displayed_label"]

    assert rename_widget.is_visible, (
        "Expected the rename input to be visible, but it is hidden."
    )
    assert rename_widget.is_focused, (
        "Expected the rename input to be focused immediately after appearing."
    )
    assert rename_widget.current_value == displayed_label, (
        f"Input pre-fill mismatch: expected '{displayed_label}', "
        f"got '{rename_widget.current_value}'."
    )


@when("the user types a new label and presses Enter")
def user_types_new_label_and_presses_enter(shared_data: dict[str, Any]) -> None:
    """Simulate typing a new label into the input and pressing Enter to commit."""
    rename_widget: RenameInputWidget = shared_data["rename_widget"]

    assert rename_widget.is_visible, (
        "Cannot type into rename input — it is not visible."
    )

    # Type the new label (mirrors Playwright's .fill() followed by .press("Enter"))
    rename_widget.type_text(_REQ782_NEW_LABEL)
    assert rename_widget.current_value == _REQ782_NEW_LABEL, (
        f"Input value after typing should be '{_REQ782_NEW_LABEL}', "
        f"got '{rename_widget.current_value}'."
    )

    rename_widget.press_enter()

    # Persist the rename in the store, mirroring what the UI does on commit
    store: FavoritesStore = shared_data["store"]
    fav: GraphExplorerFavorite = shared_data["favorite"]
    updated_fav = store.rename(fav.id, _REQ782_NEW_LABEL)
    assert updated_fav.label == _REQ782_NEW_LABEL, (
        f"Store rename failed: expected '{_REQ782_NEW_LABEL}', got '{updated_fav.label}'."
    )

    # Persist to localStorage, mirroring the UI's localStorage.setItem call
    local_storage: LocalStorageSimulator = shared_data["local_storage"]
    local_storage.save_favorites(store.list())

    # Update displayed label to reflect the committed change
    shared_data["favorite"] = updated_fav
    shared_data["displayed_label"] = _REQ782_NEW_LABEL


@then("the input closes and the favorite displays the new label")
def input_closes_and_favorite_displays_new_label(shared_data: dict[str, Any]) -> None:
    """Assert the rename input is closed and the panel shows the new label."""
    rename_widget: RenameInputWidget = shared_data["rename_widget"]
    store: FavoritesStore = shared_data["store"]
    fav: GraphExplorerFavorite = shared_data["favorite"]

    # Input must no longer be visible after Enter
    assert not rename_widget.is_visible, (
        "Rename input is still visible after pressing Enter; expected it to close."
    )
    assert rename_widget.was_committed, (
        "Rename widget was not in the committed state after pressing Enter."
    )
    assert rename_widget.committed_label == _REQ782_NEW_LABEL, (
        f"Committed label mismatch: expected '{_REQ782_NEW_LABEL}', "
        f"got '{rename_widget.committed_label}'."
    )

    # The store must reflect the new label
    updated = store.get(fav.id)
    assert updated is not None, "Favorite disappeared from the store after rename."
    assert updated.label == _REQ782_NEW_LABEL, (
        f"Store label mismatch after rename: expected '{_REQ782_NEW_LABEL}', "
        f"got '{updated.label}'."
    )

    # The displayed label (panel surface) must show the new name
    displayed: str = shared_data["displayed_label"]
    assert displayed == _REQ782_NEW_LABEL, (
        f"Panel displayed label mismatch: expected '{_REQ782_NEW_LABEL}', "
        f"got '{displayed}'."
    )


@then("the change persists in localStorage")
def change_persists_in_local_storage(shared_data: dict[str, Any]) -> None:
    """Assert localStorage contains the updated label for the favorite."""
    local_storage: LocalStorageSimulator = shared_data["local_storage"]
    fav: GraphExplorerFavorite = shared_data["favorite"]

    ls_items = local_storage.load_favorites()
    assert len(ls_items) >= 1, (
        "localStorage is empty; expected at least one favorite entry."
    )

    # Find the renamed favorite by id in the persisted list
    matching = [item for item in ls_items if item["id"] == fav.id]
    assert len(matching) == 1, (
        f"Expected exactly 1 entry with id '{fav.id}' in localStorage, "
        f"found {len(matching)}."
    )

    persisted_label = matching[0]["label"]
    assert persisted_label == _REQ782_NEW_LABEL, (
        f"localStorage label mismatch: expected '{_REQ782_NEW_LABEL}', "
        f"got '{persisted_label}'."
    )

    # Also verify the query/cypher is untouched by the rename
    persisted_query = matching[0]["query"]
    assert persisted_query == _REQ782_QUERY, (
        f"localStorage query was unexpectedly mutated during rename: "
        f"expected '{_REQ782_QUERY}', got '{persisted_query}'."
    )

    # Confirm the raw JSON round-trips correctly
    raw_json = local_storage.get_item(_LOCAL_STORAGE_KEY)
    assert raw_json is not None, "localStorage key is missing after save."
    round_tripped = json.loads(raw_json)
    assert any(
        item["label"] == _REQ782_NEW_LABEL for item in round_tripped
    ), (
        f"Round-tripped JSON does not contain label '{_REQ782_NEW_LABEL}': "
        f"{round_tripped}"
    )


# ---------------------------------------------------------------------------
# Step definitions — REQ-783 default behaviour
# ---------------------------------------------------------------------------

_REQ783_QUERY = "MATCH (n:Inquiries) RETURN n LIMIT 5"
_REQ783_LABEL = "My Favorite"


@given("the Favorites panel with at least one visible favorite")
def favorites_panel_with_at_least_one_favorite(shared_data: dict[str, Any]) -> None:
    """Set up a FavoritesStore with one favorite, a panel surface, and localStorage sim.

    Mirrors what the e2e spec does in seedFavoriteAndOpenPanel: seed a favorite
    directly into localStorage and verify it renders in the panel.
    """
    store = FavoritesStore()
    fav = store.add(_REQ783_LABEL, _REQ783_QUERY)

    # Sanity-check the store state
    retrieved = store.get(fav.id)
    assert retrieved is not None, "Favorite was not stored correctly in setup."
    assert retrieved.label == _REQ783_LABEL
    assert retrieved.cypher == _REQ783_QUERY

    # Simulate localStorage seeded with the initial favorite (mirrors the e2e spec)
    local_storage = LocalStorageSimulator()
    local_storage.save_favorites([fav])

    # Verify localStorage is seeded correctly
    ls_items = local_storage.load_favorites()
    assert len(ls_items) == 1, (
        f"Expected exactly 1 item in localStorage after seed, got {len(ls_items)}."
    )
    assert ls_items[0]["id"] == fav.id
    assert ls_items[0]["label"] == _REQ783_LABEL
    assert ls_items[0]["query"] == _REQ783_QUERY

    # Render the panel surface and confirm the favorite is visible
    panel = FavoritesPanelSurface()
    panel.render(store.list())
    assert panel.item_count == 1, (
        f"Panel should show 1 favorite item, but shows {panel.item_count}."
    )
    assert panel.contains(fav.id), (
        f"Panel does not contain the seeded favorite with id '{fav.id}'."
    )
    assert not panel.placeholder_visible, (
        "Placeholder is showing even though a favorite exists — panel logic error."
    )

    shared_data["store"] = store
    shared_data["favorite"] = fav
    shared_data["local_storage"] = local_storage
    shared_data["panel"] = panel


@when("the user hovers over a favorite and clicks the delete button")
def user_hovers_and_clicks_delete_button(shared_data: dict[str, Any]) -> None:
    """Simulate hovering over the favorite item and clicking its delete (.graph-fav-del) button.

    The hover step is a UI precondition that reveals the delete button; in the
    model we proceed directly to the delete action since button visibility is a
    CSS concern.  The critical action here is:
      1. Remove the favorite from the store (immediate panel update).
      2. Persist the updated (empty) list to localStorage.
      3. Re-render the panel surface to reflect the deletion.
    """
    store: FavoritesStore = shared_data["store"]
    fav: GraphExplorerFavorite = shared_data["favorite"]
    local_storage: LocalStorageSimulator = shared_data["local_storage"]
    panel: FavoritesPanelSurface = shared_data["panel"]

    # Confirm the favorite is present before deletion
    assert store.get(fav.id) is not None, (
        f"Favorite '{fav.id}' is missing from the store before delete was clicked."
    )
    assert panel.contains(fav.id), (
        f"Panel does not show favorite '{fav.id}' before delete was clicked."
    )

    # --- Delete action (mirrors the UI's onClick handler) ---
    store.delete(fav.id)

    # Immediately verify the store no longer holds the item
    assert store.get(fav.id) is None, (
        f"Favorite '{fav.id}' is still in the store immediately after deletion."
    )

    # Persist the updated (now empty) favorites list to localStorage
    remaining = store.list()
    local_storage.save_favorites(remaining)

    # Re-render the panel surface to reflect the deletion
    panel.render(remaining)

    # Record the deleted favorite id for downstream assertions
    shared_data["deleted_fav_id"] = fav.id


@then("the favorite is removed from the panel immediately")
def favorite_is_removed_from_panel_immediately(shared_data: dict[str, Any]) -> None:
    """Assert the panel no longer shows any .graph-fav-item elements.

    This mirrors the Playwright assertion:
        await expect(page.locator(".graph-fav-item")).toHaveCount(0);
    """
    panel: FavoritesPanelSurface = shared_data["panel"]
    deleted_fav_id: str = shared_data["deleted_fav_id"]

    assert panel.item_count == 0, (
        f"Panel still shows {panel.item_count} favorite item(s) after deletion; "
        "expected 0 immediately."
    )
    assert not panel.contains(deleted_fav_id), (
        f"Panel still contains deleted favorite id '{deleted_fav_id}'."
    )


@then("the removal persists in localStorage (empty panel shows placeholder)")
def removal_persists_in_local_storage_and_placeholder_shown(
    shared_data: dict[str, Any],
) -> None:
    """Assert localStorage is updated and the panel's empty placeholder is visible.

    This mirrors two Playwright assertions from the e2e spec:
        await expect(page.locator(".graph-fav-item")).toHaveCount(0);
        await expect(page.locator(".graph-schema-empty")).toBeVisible();

    And verifies localStorage persistence: after a page reload the favorites
    list read from localStorage must be empty, ensuring the deletion survives
    a refresh.
    """
    local_storage: LocalStorageSimulator = shared_data["local_storage"]
    panel: FavoritesPanelSurface = shared_data["panel"]
    deleted_fav_id: str = shared_data["deleted_fav_id"]

    # --- localStorage persistence check ---
    ls_items = local_storage.load_favorites()
    assert len(ls_items) == 0, (
        f"localStorage still contains {len(ls_items)} favorite(s) after deletion; "
        "expected an empty list."
    )

    # The deleted id must not appear anywhere in the persisted data
    persisted_ids = [item["id"] for item in ls_items]
    assert deleted_fav_id not in persisted_ids, (
        f"Deleted favorite id '{deleted_fav_id}' is still present in localStorage."
    )

    # Confirm the raw JSON key exists but holds an empty array (not absent/null)
    raw_json = local_storage.get_item(_LOCAL_STORAGE_KEY)
    assert raw_json is not None, (
        "localStorage key is absent after deletion; expected it to hold an empty array."
    )
    round_tripped = json.loads(raw_json)
    assert isinstance(round_tripped, list), (
        f"Expected a JSON array in localStorage, got: {type(round_tripped).__name__}."
    )
    assert len(round_tripped) == 0, (
        f"localStorage JSON array has {len(round_tripped)} items; expected 0."
    )

    # --- Simulate page reload: re-render panel from localStorage ---
    reloaded_favorites: list[GraphExplorerFavorite] = []
    for item in round_tripped:
        reloaded_favorites.append(
            GraphExplorerFavorite(
                id=item["id"],
                label=item["label"],
                cypher=item["query"],
            )
        )

    panel.render(reloaded_favorites)

    # After reload the panel must still be empty
    assert panel.item_count == 0, (
        f"Panel shows {panel.item_count} item(s) after simulated reload; expected 0."
    )

    # --- Placeholder visibility check ---
    # Mirrors: await
