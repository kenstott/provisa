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

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A Provisa read over Arrow Flight SQL (REQ-2270), without Spark: provisa-pyspark's {@code
 * flight.py}.
 *
 * <p>The table's declared schema is GetSchema of the statement (Provisa types it from the
 * registered metadata, as the principal would be answered it); its endpoints are GetFlightInfo of
 * the statement; each endpoint's rows are DoGet of its ticket. Every call carries the caller's
 * headers, so every one is governed and audited as that principal.
 *
 * <p>The one Flight SQL message sent, CommandStatementQuery, is encoded here (FlightSql.proto
 * field 1, {@code query}, wrapped in a {@code google.protobuf.Any}) rather than taking the Flight
 * SQL module for it.
 */
public final class Flight {
  private static final byte[] STATEMENT_QUERY =
      "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
          .getBytes(StandardCharsets.UTF_8);
  /** A location that says "the server this info came from" (Flight's reuse-connection URI). */
  private static final String REUSE = "arrow-flight-reuse-connection://";

  private Flight() {}

  private static void varint(ByteArrayOutputStream out, long value) {
    while (true) {
      int b = (int) (value & 0x7F);
      value >>>= 7;
      if (value != 0) {
        out.write(b | 0x80);
      } else {
        out.write(b);
        return;
      }
    }
  }

  private static byte[] lengthDelimited(int field, byte[] payload) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    varint(out, ((long) field << 3) | 2);
    varint(out, payload.length);
    out.writeBytes(payload);
    return out.toByteArray();
  }

  /** The FlightDescriptor command of a Flight SQL statement query. */
  public static byte[] statementCommand(String query) {
    byte[] message = lengthDelimited(1, query.getBytes(StandardCharsets.UTF_8));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(lengthDelimited(1, STATEMENT_QUERY));
    out.writeBytes(lengthDelimited(2, message));
    return out.toByteArray();
  }

  /** Where one part of the rows is fetched: its ticket and the server holding it. */
  public record Endpoint(byte[] ticket, String uri) implements Serializable {}

  static CallOption call(ReadOptions options) {
    FlightCallHeaders headers = new FlightCallHeaders();
    for (Map.Entry<String, String> header : options.headers().entrySet()) {
      headers.insert(header.getKey(), header.getValue());
    }
    return new HeaderCallOption(headers);
  }

  private static FlightDescriptor descriptor(ReadOptions options) {
    return FlightDescriptor.command(statementCommand(options.statement()));
  }

  static FlightClient connect(BufferAllocator allocator, String uri) {
    try {
      return FlightClient.builder(allocator, new Location(uri)).build();
    } catch (URISyntaxException e) {
      throw new ReadOptions.OptionError("option uri " + PyRepr.of(uri) + " is not a URI: " + e);
    }
  }

  /** The table's columns as the principal is served them, typed as declared. */
  public static Schema declaredSchema(ReadOptions options) {
    try (BufferAllocator allocator = new RootAllocator();
        FlightClient client = connect(allocator, options.uri())) {
      return client.getSchema(descriptor(options), call(options)).getSchema();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted closing the Flight client", e);
    }
  }

  /** The endpoints the table's rows are fetched from, each one a part of them. */
  public static List<Endpoint> endpoints(ReadOptions options) {
    FlightInfo info;
    try (BufferAllocator allocator = new RootAllocator();
        FlightClient client = connect(allocator, options.uri())) {
      info = client.getInfo(descriptor(options), call(options));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted closing the Flight client", e);
    }
    List<Endpoint> out = new ArrayList<>();
    for (FlightEndpoint endpoint : info.getEndpoints()) {
      out.add(new Endpoint(endpoint.getTicket().getBytes(), endpointUri(options, endpoint)));
    }
    return out;
  }

  /**
   * The server an endpoint's ticket is fetched from: the one the info came from when the endpoint
   * names none (or names that one), else the first location it names (Flight: any of them serves
   * the ticket).
   */
  static String endpointUri(ReadOptions options, FlightEndpoint endpoint) {
    if (endpoint.getLocations().isEmpty()) {
      return options.uri();
    }
    String uri = endpoint.getLocations().get(0).getUri().toString();
    return uri.startsWith(REUSE) ? options.uri() : uri;
  }

  /** The stream of one endpoint's record batches, as the server sends them. */
  static FlightStream stream(FlightClient client, ReadOptions options, Endpoint endpoint) {
    return client.getStream(new Ticket(endpoint.ticket()), call(options));
  }

  /** Rows arrived in a shape other than the table's declared one. */
  public static final class SchemaDrift extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public SchemaDrift(String message) {
      super(message);
    }
  }

  /**
   * Refuses a batch whose columns are not the declared ones, in order, each of a type whose Spark
   * type is the declared column's ({@code conformed}'s checks in flight.py).
   */
  public static void checkDeclared(Schema batch, Schema declared) {
    List<String> got = names(batch);
    List<String> want = names(declared);
    if (!got.equals(want)) {
      throw new SchemaDrift(
          "rows arrived with columns " + pyList(got) + ", the table declares " + pyList(want));
    }
    for (int i = 0; i < got.size(); i++) {
      Field field = batch.getFields().get(i);
      Field wanted = declared.getFields().get(i);
      SparkTypes.SparkType arrived = SparkTypes.sparkType(field, field.getName());
      SparkTypes.SparkType expected = SparkTypes.sparkType(wanted, wanted.getName());
      if (!arrived.ddl().equals(expected.ddl()) || !arrived.simple().equals(expected.simple())) {
        throw new SchemaDrift(
            "column "
                + PyRepr.of(field.getName())
                + " arrived as "
                + field.getType()
                + ", declared as "
                + wanted.getType());
      }
    }
  }

  private static List<String> names(Schema schema) {
    List<String> names = new ArrayList<>();
    for (Field field : schema.getFields()) {
      names.add(field.getName());
    }
    return names;
  }

  private static String pyList(List<String> items) {
    List<String> quoted = new ArrayList<>();
    for (String item : items) {
      quoted.add(PyRepr.of(item));
    }
    return "[" + String.join(", ", quoted) + "]";
  }
}
