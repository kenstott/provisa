# Copyright (c) 2026 Kenneth Stott
# Canary: cf5b691c-ab1c-4075-9f9e-bd1f92b02a08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A grpc_remote source's compiled stubs (REQ-329): two sources of one proto in one process.

Each source compiles its proto under its own name and loads the result. The messages a proto
declares keep their full names (package.Message) whatever the source is called, so stubs loaded
into protobuf's process-wide default pool collide on the second source: "duplicate symbol". A
second source of the same remote service, or a source added again under a new id, was refused.
"""

import uuid

import pytest

pytest.importorskip("grpc_tools")

from provisa.grpc_remote.executor import load_stubs  # noqa: E402
from provisa.grpc_remote.loader import compile_proto_stubs  # noqa: E402


def _proto(package: str) -> str:
    return f"""
syntax = "proto3";
package {package};
import "google/protobuf/timestamp.proto";

message ListBreedsRequest {{
  int32 limit = 1;
}}

message Breed {{
  string name = 1;
  int32 avg_lifespan_years = 2;
  google.protobuf.Timestamp seen = 3;
}}

service AnimalCatalog {{
  rpc ListBreeds (ListBreedsRequest) returns (stream Breed);
}}
"""


def _stubs(proto_text: str, name: str, out_dir):
    out_dir.mkdir()
    pb2_path, _ = compile_proto_stubs(proto_text, proto_name=name, out_dir=str(out_dir))
    return load_stubs(pb2_path)


def test_two_sources_of_one_proto_both_load(tmp_path):
    proto = _proto(f"t{uuid.uuid4().hex}.animal")
    first = _stubs(proto, "source_a", tmp_path / "a")
    second = _stubs(proto, "source_b", tmp_path / "b")

    for pb2 in (first, second):
        breed = pb2.Breed(name="Siamese", avg_lifespan_years=15)
        breed.seen.seconds = 7
        assert pb2.Breed.FromString(breed.SerializeToString()) == breed
        assert pb2.ListBreedsRequest(limit=2).limit == 2


def test_each_source_has_its_own_message_classes(tmp_path):
    proto = _proto(f"t{uuid.uuid4().hex}.animal")
    first = _stubs(proto, "source_a", tmp_path / "a")
    second = _stubs(proto, "source_b", tmp_path / "b")

    assert first.Breed is not second.Breed
    assert first.Breed.DESCRIPTOR.full_name == second.Breed.DESCRIPTOR.full_name


def test_a_refreshed_proto_under_the_same_name_is_the_one_loaded(tmp_path):
    # The refresh endpoint compiles a source's new proto under the source's name again.
    package = f"t{uuid.uuid4().hex}.animal"
    _stubs(_proto(package), "source_a", tmp_path / "a")
    refreshed = _proto(package).replace(
        "int32 limit = 1;", "int32 limit = 1;\n  string species = 2;"
    )
    pb2 = _stubs(refreshed, "source_a", tmp_path / "b")

    assert pb2.ListBreedsRequest(limit=1, species="cat").species == "cat"


def test_an_unknown_message_is_not_an_attribute(tmp_path):
    pb2 = _stubs(_proto(f"t{uuid.uuid4().hex}.animal"), "source_a", tmp_path / "a")

    assert getattr(pb2, "NoSuchMessage", None) is None
