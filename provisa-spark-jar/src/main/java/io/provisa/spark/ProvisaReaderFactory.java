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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** Reads each endpoint's batches as the principal, as columnar batches over Arrow vectors. */
final class ProvisaReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 1L;

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    // supportColumnarReads is true for every partition, so Spark asks only for columnar readers.
    throw new UnsupportedOperationException("a Provisa partition is read as columnar batches");
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    return new Reader((ProvisaPartition) partition);
  }

  /**
   * One endpoint's batches. A column already in the layout Spark reads its type as is wrapped as
   * it arrives; any other is copied into that layout ({@link Conform}), and the copy is freed when
   * the next batch is read.
   */
  static final class Reader implements PartitionReader<ColumnarBatch> {
    private final BufferAllocator allocator;
    private final FlightClient client;
    private final FlightStream stream;
    private final Schema declared;
    /** The layout Spark reads each declared column's type as. */
    private final List<Field> target = new ArrayList<>();
    private final List<FieldVector> copies = new ArrayList<>();
    private ColumnarBatch batch;

    Reader(ProvisaPartition partition) {
      try {
        declared = Schema.fromJSON(partition.declared());
      } catch (IOException e) {
        throw new IllegalStateException("the partition's declared schema is not Arrow JSON", e);
      }
      for (Field field : declared.getFields()) {
        target.add(Conform.target(field));
      }
      allocator = new RootAllocator();
      FlightClient opened = null;
      try {
        opened = Flight.connect(allocator, partition.endpoint().uri());
        stream = Flight.stream(opened, partition.read(), partition.endpoint());
      } catch (RuntimeException e) {
        try {
          AutoCloseables.close(opened, allocator);
        } catch (Exception closing) {
          e.addSuppressed(closing);
        }
        throw e;
      }
      client = opened;
    }

    @Override
    public boolean next() throws IOException {
      freeCopies();
      if (!stream.next()) {
        return false;
      }
      VectorSchemaRoot root = stream.getRoot();
      Flight.checkDeclared(root.getSchema(), declared);
      List<FieldVector> vectors = root.getFieldVectors();
      ColumnVector[] columns = new ColumnVector[vectors.size()];
      for (int k = 0; k < columns.length; k++) {
        FieldVector vector = vectors.get(k);
        Field want = target.get(k);
        if (Conform.sameLayout(vector.getField(), want)) {
          columns[k] = new ArrowColumnVector(vector);
        } else {
          FieldVector copy = Conform.copied(vector, want, allocator);
          copies.add(copy);
          columns[k] = new ArrowColumnVector(copy);
        }
      }
      batch = new ColumnarBatch(columns, root.getRowCount());
      return true;
    }

    @Override
    public ColumnarBatch get() {
      return batch;
    }

    private void freeCopies() {
      for (FieldVector copy : copies) {
        copy.close();
      }
      copies.clear();
    }

    @Override
    public void close() throws IOException {
      freeCopies();
      try {
        AutoCloseables.close(stream, client, allocator);
      } catch (Exception e) {
        throw new IOException("closing an endpoint's Flight read", e);
      }
    }
  }
}
