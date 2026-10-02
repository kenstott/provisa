# Copyright (c) 2026 Kenneth Stott
# Canary: 0744a6e2-b2a9-430e-bcbf-77e42619235b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model store (REQ-1919): the control plane's create, read, update and delete for every
kind of object in the model, and the only place that writes a model table.

One module per kind (``role``, ``table``, ``source``, ``domain``, …). ``integrity`` holds the
dependency guard every delete asks first, and its inventory of what refers to what (REQ-1917,
REQ-1918); a change of an object's domain asks the same inventory.

It is being made complete kind by kind. Deletes and domain changes come first: a converted
kind's delete has exactly one implementation here, called by the admin mutations, the REST
routes and the config loader alike. Creates and updates move in afterwards, and with them the
things every change to the model carries: one transaction per change, the per-object version
check and the reload stamp of REQ-1914, and the audit record. They attach to these same
functions.

``tests/unit/test_model_store_owns_writes.py`` fails the build when a module outside this
package writes a table whose operation has been converted.

The package keeps the name ``repositories`` until the terminology pass renames it.
"""

# Requirements: REQ-1919
