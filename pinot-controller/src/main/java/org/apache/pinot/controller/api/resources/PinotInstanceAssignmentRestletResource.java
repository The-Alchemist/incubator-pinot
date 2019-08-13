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
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.instance.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitionsUtils;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotInstanceAssignmentRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceAssignmentRestletResource.class);

  @Inject
  PinotHelixResourceManager _resourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions/{instancePartitionsName}")
  @ApiOperation(value = "Get the instance partitions")
  public InstancePartitions getInstancePartitions(
      @ApiParam(value = "Name of the instance partitions") @Nonnull @PathParam("instancePartitionsName") String instancePartitionsName) {
    InstancePartitions instancePartitions =
        InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getPropertyStore(), instancePartitionsName);
    if (instancePartitions == null) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the instance partitions",
          Response.Status.NOT_FOUND);
    } else {
      return instancePartitions;
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions")
  @ApiOperation(value = "Get the instance partitions for a table")
  public Map<InstancePartitionsType, InstancePartitions> getInstancePartitionsForTable(
      @ApiParam(value = "Name of the table") @Nonnull @QueryParam("tableName") String tableName) {
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();

    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      InstancePartitions offlineInstancePartitions = InstancePartitionsUtils
          .fetchInstancePartitions(_resourceManager.getPropertyStore(),
              InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName));
      if (offlineInstancePartitions != null) {
        instancePartitionsMap.put(InstancePartitionsType.OFFLINE, offlineInstancePartitions);
      }
    }
    if (tableType != TableType.OFFLINE) {
      InstancePartitions consumingInstancePartitions = InstancePartitionsUtils
          .fetchInstancePartitions(_resourceManager.getPropertyStore(),
              InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
      if (consumingInstancePartitions != null) {
        instancePartitionsMap.put(InstancePartitionsType.CONSUMING, consumingInstancePartitions);
      }
      InstancePartitions completedInstancePartitions = InstancePartitionsUtils
          .fetchInstancePartitions(_resourceManager.getPropertyStore(),
              InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
      if (completedInstancePartitions != null) {
        instancePartitionsMap.put(InstancePartitionsType.COMPLETED, completedInstancePartitions);
      }
    }

    if (instancePartitionsMap.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the instance partitions",
          Response.Status.NOT_FOUND);
    } else {
      return instancePartitionsMap;
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/assignInstances")
  @ApiOperation(value = "Assign server instances to a table")
  public Map<InstancePartitionsType, InstancePartitions> assignInstances(
      @ApiParam(value = "Name of the table") @Nonnull @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED") @QueryParam("type") InstancePartitionsType instancePartitionsType,
      @ApiParam(value = "Whether to do dry-run") @DefaultValue("false") @QueryParam("dryRun") boolean dryRun) {
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    List<InstanceConfig> instanceConfigs = _resourceManager.getAllHelixInstanceConfigs();

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME && (instancePartitionsType == InstancePartitionsType.OFFLINE
        || instancePartitionsType == null)) {
      TableConfig offlineTableConfig = _resourceManager.getOfflineTableConfig(tableName);
      if (offlineTableConfig != null) {
        try {
          if (InstanceAssignmentConfigUtils
              .allowInstanceAssignment(offlineTableConfig, InstancePartitionsType.OFFLINE)) {
            instancePartitionsMap.put(InstancePartitionsType.OFFLINE, new InstanceAssignmentDriver(offlineTableConfig)
                .assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs));
          }
        } catch (IllegalStateException e) {
          throw new ControllerApplicationException(LOGGER, "Caught IllegalStateException", Response.Status.BAD_REQUEST,
              e);
        } catch (Exception e) {
          throw new ControllerApplicationException(LOGGER, "Caught exception while calculating the instance partitions",
              Response.Status.INTERNAL_SERVER_ERROR, e);
        }
      }
    }
    if (tableType != TableType.OFFLINE && instancePartitionsType != InstancePartitionsType.OFFLINE) {
      TableConfig realtimeTableConfig = _resourceManager.getRealtimeTableConfig(tableName);
      if (realtimeTableConfig != null) {
        try {
          InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(realtimeTableConfig);
          if (instancePartitionsType == InstancePartitionsType.CONSUMING || instancePartitionsType == null) {
            if (InstanceAssignmentConfigUtils
                .allowInstanceAssignment(realtimeTableConfig, InstancePartitionsType.CONSUMING)) {
              instancePartitionsMap.put(InstancePartitionsType.CONSUMING,
                  instanceAssignmentDriver.assignInstances(InstancePartitionsType.CONSUMING, instanceConfigs));
            }
          }
          if (instancePartitionsType == InstancePartitionsType.COMPLETED || instancePartitionsType == null) {
            if (InstanceAssignmentConfigUtils
                .allowInstanceAssignment(realtimeTableConfig, InstancePartitionsType.COMPLETED)) {
              instancePartitionsMap.put(InstancePartitionsType.COMPLETED,
                  instanceAssignmentDriver.assignInstances(InstancePartitionsType.COMPLETED, instanceConfigs));
            }
          }
        } catch (IllegalStateException e) {
          throw new ControllerApplicationException(LOGGER, "Caught IllegalStateException", Response.Status.BAD_REQUEST,
              e);
        } catch (Exception e) {
          throw new ControllerApplicationException(LOGGER, "Caught exception while calculating the instance partitions",
              Response.Status.INTERNAL_SERVER_ERROR, e);
        }
      }
    }

    if (instancePartitionsMap.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the instance assignment config",
          Response.Status.NOT_FOUND);
    }

    if (!dryRun) {
      for (InstancePartitions instancePartitions : instancePartitionsMap.values()) {
        persistInstancePartitionsHelper(instancePartitions);
      }
    }

    return instancePartitionsMap;
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions")
  @ApiOperation(value = "Create/update the instance partitions")
  public InstancePartitions putInstancePartitions(String instancePartitionsStr) {
    InstancePartitions instancePartitions;
    try {
      instancePartitions = JsonUtils.stringToObject(instancePartitionsStr, InstancePartitions.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to deserialize the instance partitions",
          Response.Status.BAD_REQUEST);
    }

    persistInstancePartitionsHelper(instancePartitions);

    return instancePartitions;
  }

  private void persistInstancePartitionsHelper(InstancePartitions instancePartitions) {
    try {
      LOGGER.info("Persisting instance partitions: {} to the property store", instancePartitions.getName());
      InstancePartitionsUtils.persistInstancePartitions(_resourceManager.getPropertyStore(), instancePartitions);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Caught Exception while persisting the instance partitions",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions/{instancePartitionsName}")
  @ApiOperation(value = "Remove the instance partitions")
  public SuccessResponse removeInstancePartitions(
      @ApiParam(value = "Name of the instance partitions") @Nonnull @PathParam("instancePartitionsName") String instancePartitionsName) {
    removeInstancePartitionsHelper(instancePartitionsName);
    return new SuccessResponse("Instance partitions removed");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions")
  @ApiOperation(value = "Remove the instance partitions for a table")
  public SuccessResponse removeInstancePartitionsForTable(
      @ApiParam(value = "Name of the table") @Nonnull @QueryParam("tableName") String tableNameWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    if (tableType != TableType.REALTIME) {
      removeInstancePartitionsHelper(InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName));
    }
    if (tableType != TableType.OFFLINE) {
      removeInstancePartitionsHelper(InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
      removeInstancePartitionsHelper(InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
    }
    return new SuccessResponse("Instance partitions removed");
  }

  private void removeInstancePartitionsHelper(String instancePartitionsName) {
    try {
      LOGGER.info("Removing instance partitions: {} from the property store", instancePartitionsName);
      InstancePartitionsUtils.removeInstancePartitions(_resourceManager.getPropertyStore(), instancePartitionsName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Caught Exception while removing the instance partitions",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions/replaceInstance/{instancePartitionsName}")
  @ApiOperation(value = "Replace an instance in the instance partitions")
  public InstancePartitions replaceInstance(
      @ApiParam(value = "Name of the instance partitions") @Nonnull @PathParam("instancePartitionsName") String instancePartitionsName,
      @ApiParam(value = "Old instance to be replaced") @Nonnull @QueryParam("oldInstanceId") String oldInstanceId,
      @ApiParam(value = "New instance to replace with") @Nonnull @QueryParam("newInstanceId") String newInstanceId) {
    InstancePartitions instancePartitions = getInstancePartitions(instancePartitionsName);
    boolean oldInstanceFound = false;
    Map<String, List<String>> partitionToInstancesMap = instancePartitions.getPartitionToInstancesMap();
    for (List<String> instances : partitionToInstancesMap.values()) {
      oldInstanceFound |= Collections.replaceAll(instances, oldInstanceId, newInstanceId);
    }
    if (!oldInstanceFound) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the old instance", Response.Status.NOT_FOUND);
    } else {
      persistInstancePartitionsHelper(instancePartitions);
      return instancePartitions;
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/instancePartitions/replaceInstance")
  @ApiOperation(value = "Replace an instance in the instance partitions for a table")
  public Map<InstancePartitionsType, InstancePartitions> replaceInstanceForTable(
      @ApiParam(value = "Name of the table") @Nonnull @QueryParam("tableName") String tableName,
      @ApiParam(value = "Old instance to be replaced") @Nonnull @QueryParam("oldInstanceId") String oldInstanceId,
      @ApiParam(value = "New instance to replace with") @Nonnull @QueryParam("newInstanceId") String newInstanceId) {
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = getInstancePartitionsForTable(tableName);
    Iterator<InstancePartitions> iterator = instancePartitionsMap.values().iterator();
    while (iterator.hasNext()) {
      InstancePartitions instancePartitions = iterator.next();
      boolean oldInstanceFound = false;
      Map<String, List<String>> partitionToInstancesMap = instancePartitions.getPartitionToInstancesMap();
      for (List<String> instances : partitionToInstancesMap.values()) {
        oldInstanceFound |= Collections.replaceAll(instances, oldInstanceId, newInstanceId);
      }
      if (oldInstanceFound) {
        persistInstancePartitionsHelper(instancePartitions);
      } else {
        iterator.remove();
      }
    }
    if (instancePartitionsMap.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the old instance", Response.Status.NOT_FOUND);
    } else {
      return instancePartitionsMap;
    }
  }
}
