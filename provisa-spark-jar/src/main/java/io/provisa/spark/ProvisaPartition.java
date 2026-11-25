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

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * One Flight endpoint of the statement, with what its executor needs to read it as the principal:
 * the read's options and the declared Arrow schema (as JSON: Arrow's schema is not serializable).
 */
record ProvisaPartition(ReadOptions read, Flight.Endpoint endpoint, String declared)
    implements InputPartition {}
