# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e7c41-5d38-4a6f-8e17-c04a6d3b92f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Import an ontology made outside Provisa as an alignment (REQ-2237).

An Apache Ossie ontology or an RDF/OWL file (Turtle, RDF/XML, N-Triples, JSON-LD), such as an
industry standard like FIBO, is read by :mod:`.parse` into items: its classes, properties, value
types and concepts, each with its definition, IRI and alternative labels, and the structure the
file states between them. :mod:`.align` proposes how each item aligns with the terms the glossary
already holds, by the rule the glossary derives terms with (provisa.core.glossary.normalize_term):
the same entity, a synonym, a kind of, a part of. Nothing is linked field by field: an item
grounds through the model's own columns and tables only by way of the terms it aligns with.
:mod:`.store` keeps an import and its proposals for review, and :mod:`.apply` carries out a
steward's decisions: every item becomes a draft term carrying its definition and IRI (or, aligned
as the same entity, the existing term takes the IRI), and every approved alignment becomes an
edge. Nothing is approved on a steward's behalf, and a term aligned with nothing stays a Concept.

A whole release -- every module file of it in one zip archive -- is read by :mod:`.release`,
its own ``owl:imports`` resolved inside the archive and nothing fetched. What an ontology states
that a term does not model is kept on the term (:mod:`.kept`), shown on its page, and written
back out by :mod:`.export` (RDF) and provisa.ossie.ontology (Ossie), so an import round-trips.
"""

# Requirements: REQ-2237
