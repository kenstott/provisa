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

import com.google.protobuf.Any;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A Flight server on a loopback port standing in for Provisa: it answers the declared schema,
 * one endpoint, and that endpoint's rows (written by the test), and records each call's method,
 * statement and caller headers.
 */
final class StubProvisa implements AutoCloseable {
  /** One call: its method, the statement it carried (or the ticket's text) and its headers. */
  record Call(String method, String statement, String authorization, String role) {}

  private static final FlightServerMiddleware.Key<Headers> KEY =
      FlightServerMiddleware.Key.of("headers");

  /** The headers a call arrived with. */
  private static final class Headers implements FlightServerMiddleware {
    final CallHeaders incoming;

    Headers(CallHeaders incoming) {
      this.incoming = incoming;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoing) {}

    @Override
    public void onCallCompleted(CallStatus status) {}

    @Override
    public void onCallErrored(Throwable err) {}
  }

  final List<Call> calls = new CopyOnWriteArrayList<>();
  final BufferAllocator allocator = new RootAllocator();
  private final FlightServer server;

  StubProvisa(Schema declared, Schema rows, Consumer<VectorSchemaRoot> fill) throws IOException {
    FlightServerMiddleware.Factory<Headers> headers =
        (CallInfo info, CallHeaders incoming, RequestContext context) -> new Headers(incoming);
    NoOpFlightProducer producer =
        new NoOpFlightProducer() {
          private void record(CallContext context, String method, String statement) {
            CallHeaders h = context.getMiddleware(KEY).incoming;
            calls.add(new Call(method, statement, h.get("authorization"), h.get("x-provisa-role")));
          }

          @Override
          public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
            record(context, "schema", statement(descriptor.getCommand()));
            return new SchemaResult(declared);
          }

          @Override
          public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            String statement = statement(descriptor.getCommand());
            record(context, "info", statement);
            return new FlightInfo(
                declared,
                descriptor,
                List.of(new FlightEndpoint(new Ticket("rows".getBytes()))),
                -1,
                -1);
          }

          @Override
          public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            record(context, "rows", new String(ticket.getBytes()));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(rows, allocator)) {
              listener.start(root);
              fill.accept(root);
              listener.putNext();
              listener.completed();
            }
          }
        };
    server =
        FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer)
            .middleware(KEY, headers)
            .build()
            .start();
  }

  String uri() {
    return "grpc://127.0.0.1:" + server.getPort();
  }

  /** The query of a Flight SQL CommandStatementQuery, packed in a google.protobuf.Any. */
  static String statement(byte[] command) {
    try {
      Any any = Any.parseFrom(command);
      if (!any.getTypeUrl()
          .equals("type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery")) {
        throw new IllegalStateException("not a statement query: " + any.getTypeUrl());
      }
      CodedInputStream message = any.getValue().newCodedInput();
      int tag = message.readTag();
      if (tag != (1 << 3 | 2)) {
        throw new IllegalStateException("field 1 (query) is not first: tag " + tag);
      }
      String query = message.readString();
      if (!message.isAtEnd()) {
        throw new IllegalStateException("the statement holds more than its query");
      }
      return query;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() throws Exception {
    server.shutdown();
    server.awaitTermination();
    server.close();
    allocator.close();
  }
}
