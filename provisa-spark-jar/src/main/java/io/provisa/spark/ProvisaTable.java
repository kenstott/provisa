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

import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** One governed Provisa table, read as a batch: every column the principal is served. */
final class ProvisaTable implements SupportsRead {
  private final ReadOptions read;
  private final Schema declared;
  private final StructType schema;

  ProvisaTable(ReadOptions read, Schema declared, StructType schema) {
    this.read = read;
    this.declared = declared;
    this.schema = schema;
  }

  @Override
  public String name() {
    return String.join(".", read.table());
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new ProvisaScan(read, declared, schema);
  }

  /** The scan of the whole table: its partitions are the statement's Flight endpoints. */
  static final class ProvisaScan implements ScanBuilder, Scan, Batch {
    private final ReadOptions read;
    private final String declared;
    private final StructType schema;

    ProvisaScan(ReadOptions read, Schema declared, StructType schema) {
      this.read = read;
      this.declared = declared.toJson();
      this.schema = schema;
    }

    @Override
    public Scan build() {
      return this;
    }

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public String description() {
      return "provisa " + String.join(".", read.table());
    }

    @Override
    public Batch toBatch() {
      return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      List<Flight.Endpoint> endpoints = Flight.endpoints(read);
      InputPartition[] partitions = new InputPartition[endpoints.size()];
      for (int i = 0; i < partitions.length; i++) {
        partitions[i] = new ProvisaPartition(read, endpoints.get(i), declared);
      }
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new ProvisaReaderFactory();
    }
  }
}
