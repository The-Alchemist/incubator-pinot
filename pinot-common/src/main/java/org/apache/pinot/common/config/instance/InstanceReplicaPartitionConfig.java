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
package org.apache.pinot.common.config.instance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.common.config.ConfigDoc;
import org.apache.pinot.common.config.ConfigKey;


@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceReplicaPartitionConfig {

  @ConfigKey("replicaGroupBased")
  @ConfigDoc("Whether to use replica-group based selection, false by default")
  private boolean _replicaGroupBased;

  @ConfigKey("numServers")
  @ConfigDoc("Number of instances to select for non-replica-group based selection, select all instances if not specified")
  private int _numInstances;

  @ConfigKey("numReplicas")
  @ConfigDoc("Number of replicas (replica-groups) for replica-group based selection")
  private int _numReplicas;

  @ConfigKey("numServersPerReplica")
  @ConfigDoc("Number of instances per replica for replica-group based selection, select as many instances as possible if not specified")
  private int _numInstancesPerReplica;

  @ConfigKey("numPartitions")
  @ConfigDoc("Number of partitions for replica-group based selection, do not partition the replica-group (1 partition) if not specified")
  private int _numPartitions;

  @ConfigKey("numServersPerPartition")
  @ConfigDoc("Number of instances per partition (within a replica) for replica-group based selection, select all instances if not specified")
  private int _numInstancesPerPartition;

  @JsonProperty
  public boolean isReplicaGroupBased() {
    return _replicaGroupBased;
  }

  @JsonProperty
  public void setReplicaGroupBased(boolean replicaGroupBased) {
    _replicaGroupBased = replicaGroupBased;
  }

  @JsonProperty
  public int getNumInstances() {
    return _numInstances;
  }

  @JsonProperty
  public void setNumInstances(int numInstances) {
    _numInstances = numInstances;
  }

  @JsonProperty
  public int getNumReplicas() {
    return _numReplicas;
  }

  @JsonProperty
  public void setNumReplicas(int numReplicas) {
    _numReplicas = numReplicas;
  }

  @JsonProperty
  public int getNumInstancesPerReplica() {
    return _numInstancesPerReplica;
  }

  @JsonProperty
  public void setNumInstancesPerReplica(int numInstancesPerReplica) {
    _numInstancesPerReplica = numInstancesPerReplica;
  }

  @JsonProperty
  public int getNumPartitions() {
    return _numPartitions;
  }

  @JsonProperty
  public void setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
  }

  @JsonProperty
  public int getNumInstancesPerPartition() {
    return _numInstancesPerPartition;
  }

  @JsonProperty
  public void setNumInstancesPerPartition(int numInstancesPerPartition) {
    _numInstancesPerPartition = numInstancesPerPartition;
  }
}
