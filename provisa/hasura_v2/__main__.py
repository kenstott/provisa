# Copyright (c) 2025 Kenneth Stott
# Canary: 8ed37158-7e51-494f-839c-838f88a17cb8
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Allow running as: python -m provisa.hasura_v2"""

import sys

from provisa.hasura_v2.cli import main

sys.exit(main())
