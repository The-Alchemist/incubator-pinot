/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.hadoop.job;

/**
 * Internal-only constants for Hadoop MapReduce jobs. These constants are propagated across different segment creation
 * jobs. They are not meant to be set externally.
 */
public class InternalConfigConstants {
  public static final String TIME_COLUMN_CONFIG = "time.column";
  public static final String TIME_COLUMN_VALUE = "time.column.value";
  public static final String IS_APPEND = "is.append";
  public static final String SEGMENT_PUSH_FREQUENCY = "segment.push.frequency";
  public static final String SEGMENT_TIME_TYPE = "segment.time.type";
  public static final String SEGMENT_TIME_FORMAT = "segment.time.format";

  // Partitioning configs
  public static final String PARTITION_COLUMN_CONFIG = "partition.column";
  public static final String NUM_PARTITIONS_CONFIG = "num.partitions";
  public static final String PARTITION_FUNCTION_CONFIG = "partition.function";

  public static final String SORTED_COLUMN_CONFIG = "sorted.column";
}
