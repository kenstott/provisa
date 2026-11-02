# Copyright (c) 2026 Kenneth Stott
# Canary: 000bc363-bed7-4a3b-b8cc-0326366890f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The control plane and the data plane (REQ-1916).

The one query pipeline has two halves: the plan half (``_govern_and_route`` and its compiled
twin: govern, route, bind the org's ceilings) and the execute half (``_execute_plan`` and the
streaming terminals). The plan half is a facade: in ``every`` and ``control`` mode it runs in
process, exactly as it always has; in ``data`` mode it is a call to a control node over the plane
socket (:mod:`provisa.planes.facade`), which answers a plan signed with a key every node of the
deployment derives from its master key (:mod:`provisa.planes.signing`). The data node runs that
plan and encodes the answer itself, so rows never enter the control process, and it reports what
the control plane records (audit facts: row counts, the route, the row limit's outcome) back
over the same socket (:mod:`provisa.planes.audit_report`).

The socket is Arrow Flight actions (:mod:`provisa.planes.control_server`): Flight is already a
listener every Provisa process can run, an action is one signed request and one signed answer
with no generated stubs to keep in step, and the same server is where the later slices put data
node registration and the commands a control node sends a data node.
"""

# Requirements: REQ-1916
