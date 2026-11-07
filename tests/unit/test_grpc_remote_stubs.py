# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A gRPC remote source's compiled messages are its own: two sources built from one .proto, and
a source whose .proto is refreshed with its message names kept, each load (REQ-329, REQ-617)."""

# Requirements: REQ-329, REQ-617

from provisa.grpc_remote.executor import load_stubs
from provisa.grpc_remote.loader import compile_proto_stubs

# A package of its own: no other test's compile may have registered these names anywhere.
CATALOG = """
syntax = "proto3";
package provisa.test.stubs_isolation;
import "google/protobuf/timestamp.proto";

message ListThingsRequest { int32 limit = 1; }
message Thing { string name = 1; google.protobuf.Timestamp seen = 2; }
message ListThingsResponse { repeated Thing things = 1; }

service Things {
  rpc ListThings (ListThingsRequest) returns (ListThingsResponse);
}
"""

REFRESHED = CATALOG.replace(
    "message Thing { string name = 1;", "message Thing { string name = 1; string colour = 3;"
)


def _load(text: str, name: str, out_dir: str):
    pb2_path, _ = compile_proto_stubs(text, proto_name=name, out_dir=out_dir)
    return load_stubs(pb2_path)


def test_two_sources_from_one_proto_each_load(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    first = _load(CATALOG, "src_a", str(tmp_path / "a"))
    second = _load(CATALOG, "src_b", str(tmp_path / "b"))
    for pb2 in (first, second):
        reply = pb2.ListThingsResponse(things=[pb2.Thing(name="x")])
        assert pb2.ListThingsResponse.FromString(reply.SerializeToString()).things[0].name == "x"


def test_a_refreshed_proto_keeping_its_names_loads_its_new_field(tmp_path):
    (tmp_path / "old").mkdir()
    (tmp_path / "new").mkdir()
    old = _load(CATALOG, "src_r", str(tmp_path / "old"))
    new = _load(REFRESHED, "src_r", str(tmp_path / "new"))
    assert "colour" not in old.Thing.DESCRIPTOR.fields_by_name
    assert new.Thing(name="x", colour="red").colour == "red"


def test_a_well_known_type_it_imports_is_usable(tmp_path):
    pb2 = _load(CATALOG, "src_t", str(tmp_path))
    thing = pb2.Thing(name="x")
    thing.seen.seconds = 7
    assert pb2.Thing.FromString(thing.SerializeToString()).seen.seconds == 7
