# Prose Quality Rules

Used by the doc-writer agent and the PostToolUse prose hook.

---

## Banned Phrases

Never use these. They signal AI authorship immediately.

**Metaphor abuse:**
load-bearing, dressed up as, boils down to, at its core, the heart of,
tapestry, landscape, realm, facet, ecosystem,
unlock, harness, navigate, elevate, empower

**Hollow intensifiers:**
robust, comprehensive, holistic, nuanced, multifaceted,
seamless, streamlined, cutting-edge, state-of-the-art,
pivotal, myriad, plethora, synergy, leverage, innovative

**Filler verbs:**
delve, dive into, unpack (used figuratively), foster, facilitate (when a plain verb works)

**Transitional filler:**
"That being said", "With that in mind", "Moving forward",
"At the end of the day", "In today's X", "As we navigate",
"It's worth noting", "It should be noted", "It's important to note",
"Not just X but Y", "The intersection of", "testament to",
"In conclusion", "To summarize", "As we've discussed"

---

## Structural Rules

**No default-three lists.** A triad is a rhetorical device — three parallel clauses used for deliberate rhythmic effect ("veni, vidi, vici"). That's a choice, not a default. AI prose defaults to exactly three items in lists because three *feels* complete. Pick the count the content requires. Listing four things as three by merging two, or padding to three when two suffice, is the failure mode to avoid.

**No throat-clearing openers.** Never start a section or paragraph with "This section describes...", "The following explains...", or "In this document you will find...".

**No empty summarization.** Closing paragraphs that restate what came before add nothing. Cut them.

**No artificial balance.** Don't both-sides a topic that isn't genuinely contested.

---

## Sentence Rhythm

**Vary length.** AI prose runs every sentence 15–25 words. Mix short ones with long ones deliberately.

Short sentences land hard. Use them.

Longer sentences work when they build through subordination before arriving at the point — the reader follows the logic as it assembles, which is different from just reading a list of facts end-to-end.

**Vary construction.** Don't open every sentence with subject-verb. Options:
- Fragment. For emphasis.
- Dependent clause first: "When the cache is warm, latency drops."
- Prepositional opener: "At query time, the index is read-only."
- Question: "Why does this matter?"
- Inversion: "Rarely does a single parameter affect this much."

**Test:** Read five consecutive sentences aloud. If they all have the same rhythm, rewrite two of them.

---

## Voice

Write in active voice with a clear agent. "We recommend X" beats "X is recommended." If there's no clear agent, find one or cut the sentence.

Commit to positions. "Use X for Y" beats "X may be appropriate in some Y scenarios depending on your requirements."

Specificity over abstraction. Name the function, the error, the file. Generic examples ("a company might...") are a tell.
