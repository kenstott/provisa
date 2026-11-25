// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2270

package io.provisa.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.List;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** The Flight SQL statement and calls of a read, without Spark. */
class FlightTest {
  static final Schema DECLARED =
      new Schema(
          List.of(
              Field.nullable("id", new ArrowType.Int(32, true)),
              Field.nullable("region", ArrowType.Utf8.INSTANCE),
              Field.nullable("amount", new ArrowType.Decimal(10, 2, 128))));

  @Test
  void theStatementCommandIsTheFlightSqlMessageProvisaReads() {
    // A length past one varint byte, and a character past one UTF-8 byte.
    String query = "SELECT * FROM \"sales\".\"orders\" -- é " + "x".repeat(300);
    assertEquals(query, StubProvisa.statement(Flight.statementCommand(query)));
  }

  @Test
  void theDeclaredSchemaAndEachEndpointAreAskedForAsTheCaller() throws Exception {
    Schema declared =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("region", ArrowType.Utf8.INSTANCE)));
    try (StubProvisa provisa = new StubProvisa(declared, declared, root -> root.setRowCount(0))) {
      ReadOptions read =
          ReadOptions.parse(
              ReadOptionsTest.map(
                  "uri", provisa.uri(), "token", "pat-1", "role", "clerk", "path", "sales.orders"));
      assertEquals(declared, Flight.declaredSchema(read));
      List<Flight.Endpoint> endpoints = Flight.endpoints(read);
      assertEquals(1, endpoints.size());
      assertEquals(provisa.uri(), endpoints.get(0).uri());
      assertEquals("rows", new String(endpoints.get(0).ticket()));
      String statement = "SELECT * FROM \"sales\".\"orders\"";
      assertEquals(
          List.of(
              new StubProvisa.Call("schema", statement, "Bearer pat-1", "clerk"),
              new StubProvisa.Call("info", statement, "Bearer pat-1", "clerk")),
          provisa.calls);
    }
  }

  @Test
  void anEndpointNamingAnotherServerIsReadThere() throws Exception {
    ReadOptions read =
        ReadOptions.parse(ReadOptionsTest.map("uri", ReadOptionsTest.URI, "path", "orders"));
    Ticket t = new Ticket("t".getBytes());
    FlightEndpoint elsewhere = new FlightEndpoint(t, new Location("grpc+tls://other:8815"));
    FlightEndpoint reuse =
        new FlightEndpoint(t, new Location(new URI("arrow-flight-reuse-connection://?")));
    FlightEndpoint none = new FlightEndpoint(t);
    assertEquals("grpc+tls://other:8815", Flight.endpointUri(read, elsewhere));
    assertEquals(ReadOptionsTest.URI, Flight.endpointUri(read, reuse));
    assertEquals(ReadOptionsTest.URI, Flight.endpointUri(read, none));
  }

  @Test
  void rowsOfAnotherTypeThanDeclaredAreRefusedByColumn() {
    Schema batch =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("region", ArrowType.LargeUtf8.INSTANCE),
                Field.nullable(
                    "amount", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    Flight.SchemaDrift error =
        assertThrows(Flight.SchemaDrift.class, () -> Flight.checkDeclared(batch, DECLARED));
    assertEquals(
        "column 'amount' arrived as FloatingPoint(DOUBLE), declared as Decimal(10, 2, 128)",
        error.getMessage());
  }

  @Test
  void rowsWithOtherColumnsThanDeclaredAreRefused() {
    Schema batch = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
    Flight.SchemaDrift error =
        assertThrows(Flight.SchemaDrift.class, () -> Flight.checkDeclared(batch, DECLARED));
    assertEquals(
        "rows arrived with columns ['id'], the table declares ['id', 'region', 'amount']",
        error.getMessage());
  }

  @Test
  void rowsInAnotherLayoutOfTheDeclaredTypesAreAccepted() {
    Schema batch =
        new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("region", ArrowType.Utf8View.INSTANCE),
                Field.nullable("amount", new ArrowType.Decimal(10, 2, 256))));
    Flight.checkDeclared(batch, DECLARED);
  }
}
