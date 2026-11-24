# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b7c14-6d2a-4f85-a0c3-91d7e5b2f468
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194: building inflection's noun maps never builds lemminflect's whole lexicon.

Each load runs in a fresh interpreter, which reports how far its peak RSS (ru_maxrss) rose
across the load: streaming the lookup files row by row (provisa.core.inflection) against
lemminflect's codecs, which build every part of speech of every word first (the 42 MB boot
transient). Run with ``-rP`` to see the figures.
"""

# Requirements: REQ-194

import subprocess
import sys

_MEASURE = """
import gc, resource, sys
from lemminflect import config
from lemminflect.codecs.InflectionLUCodec import InflectionLUCodec
from lemminflect.codecs.LemmaLUCodec import LemmaLUCodec
from provisa.core import inflection

def status_kib(field):
    with open("/proc/self/status") as f:
        return next(int(line.split()[1]) for line in f if line.startswith(field + ":"))

gc.collect()
if sys.platform == "linux":
    # The import's peak is above the load's, so reset the high-water mark (VmHWM) to the
    # current RSS and measure from there.
    with open("/proc/self/clear_refs", "w") as f:
        f.write("5")
    before = status_kib("VmRSS") * 1024
else:
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS
if sys.argv[1] == "stream":
    inflection._lemminflect_nouns()
else:
    lemmas = LemmaLUCodec.load(config.lemma_lu_fn)
    del lemmas
    inflections = InflectionLUCodec.load(config.inflection_lu_fn)
    del inflections
if sys.platform == "linux":
    after = status_kib("VmHWM") * 1024
else:
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(after - before)
"""


def _peak_rise(mode: str) -> int:
    out = subprocess.run(
        [sys.executable, "-c", _MEASURE, mode], capture_output=True, text=True, check=True
    )
    return int(out.stdout.strip())


def test_streamed_noun_maps_peak_well_below_the_codec_load():
    streamed = _peak_rise("stream")
    codecs = _peak_rise("codecs")
    print(f"peak RSS rise: streamed {streamed / 2**20:.1f} MiB, codecs {codecs / 2**20:.1f} MiB")
    assert codecs > 20 * 2**20, codecs
    assert streamed < codecs / 3, (streamed, codecs)
