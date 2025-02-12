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
package org.apache.pinot.core.query.reduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.query.ReduceService;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.HavingFilterQuery;
import org.apache.pinot.common.request.HavingFilterQueryMap;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.query.selection.SelectionOperatorService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService implements ReduceService<BrokerResponseNative> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  @Nonnull
  @Override
  public BrokerResponseNative reduceOnDataTable(@Nonnull BrokerRequest brokerRequest,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap, @Nullable BrokerMetrics brokerMetrics) {
    if (dataTableMap.size() == 0) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    long numDocsScanned = 0L;
    long numEntriesScannedInFilter = 0L;
    long numEntriesScannedPostFilter = 0L;
    long numSegmentsQueried = 0L;
    long numSegmentsProcessed = 0L;
    long numSegmentsMatched = 0L;
    long numConsumingSegmentsProcessed = 0L;
    long minConsumingFreshnessTimeMs = Long.MAX_VALUE;
    long numTotalRawDocs = 0L;
    boolean numGroupsLimitReached = false;

    // Cache a data schema from data tables (try to cache one with data rows associated with it).
    DataSchema cachedDataSchema = null;

    // Process server response metadata.
    Iterator<Map.Entry<ServerInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerInstance, DataTable> entry = iterator.next();
      ServerInstance serverInstance = entry.getKey();
      DataTable dataTable = entry.getValue();
      Map<String, String> metadata = dataTable.getMetadata();

      // Reduce on trace info.
      if (brokerRequest.isEnableTrace()) {
        brokerResponseNative.getTraceInfo()
            .put(serverInstance.getHostname(), metadata.get(DataTable.TRACE_INFO_METADATA_KEY));
      }

      // Reduce on exceptions.
      for (String key : metadata.keySet()) {
        if (key.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
          processingExceptions.add(new QueryProcessingException(Integer.parseInt(key.substring(9)), metadata.get(key)));
        }
      }

      // Reduce on execution statistics.
      String numDocsScannedString = metadata.get(DataTable.NUM_DOCS_SCANNED_METADATA_KEY);
      if (numDocsScannedString != null) {
        numDocsScanned += Long.parseLong(numDocsScannedString);
      }
      String numEntriesScannedInFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY);
      if (numEntriesScannedInFilterString != null) {
        numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
      }
      String numEntriesScannedPostFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY);
      if (numEntriesScannedPostFilterString != null) {
        numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
      }
      String numSegmentsQueriedString = metadata.get(DataTable.NUM_SEGMENTS_QUERIED);
      if (numSegmentsQueriedString != null) {
        numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
      }

      String numSegmentsProcessedString = metadata.get(DataTable.NUM_SEGMENTS_PROCESSED);
      if (numSegmentsProcessedString != null) {
        numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
      }
      String numSegmentsMatchedString = metadata.get(DataTable.NUM_SEGMENTS_MATCHED);
      if (numSegmentsMatchedString != null) {
        numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
      }

      String numConsumingString = metadata.get(DataTable.NUM_CONSUMING_SEGMENTS_PROCESSED);
      if (numConsumingString != null) {
        numConsumingSegmentsProcessed += Long.parseLong(numConsumingString);
      }

      String minConsumingFreshnessTimeMsString = metadata.get(DataTable.MIN_CONSUMING_FRESHNESS_TIME_MS);
      if (minConsumingFreshnessTimeMsString != null) {
          minConsumingFreshnessTimeMs = Math.min(Long.parseLong(minConsumingFreshnessTimeMsString), minConsumingFreshnessTimeMs);
      }

      String numTotalRawDocsString = metadata.get(DataTable.TOTAL_DOCS_METADATA_KEY);
      if (numTotalRawDocsString != null) {
        numTotalRawDocs += Long.parseLong(numTotalRawDocsString);
      }
      numGroupsLimitReached |= Boolean.valueOf(metadata.get(DataTable.NUM_GROUPS_LIMIT_REACHED_KEY));

      // After processing the metadata, remove data tables without data rows inside.
      DataSchema dataSchema = dataTable.getDataSchema();
      if (dataSchema == null) {
        iterator.remove();
      } else {
        // Try to cache a data table with data rows inside, or cache one with data schema inside.
        if (dataTable.getNumberOfRows() == 0) {
          if (cachedDataSchema == null) {
            cachedDataSchema = dataSchema;
          }
          iterator.remove();
        } else {
          cachedDataSchema = dataSchema;
        }
      }
    }

    // Set execution statistics.
    brokerResponseNative.setNumDocsScanned(numDocsScanned);
    brokerResponseNative.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
    brokerResponseNative.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
    brokerResponseNative.setNumSegmentsQueried(numSegmentsQueried);
    brokerResponseNative.setNumSegmentsProcessed(numSegmentsProcessed);
    brokerResponseNative.setNumSegmentsMatched(numSegmentsMatched);
    brokerResponseNative.setTotalDocs(numTotalRawDocs);
    brokerResponseNative.setNumGroupsLimitReached(numGroupsLimitReached);
    if (numConsumingSegmentsProcessed > 0) {
      brokerResponseNative.setNumConsumingSegmentsQueried(numConsumingSegmentsProcessed);
      brokerResponseNative.setMinConsumingFreshnessTimeMs(minConsumingFreshnessTimeMs);
    }

    // Update broker metrics.
    String tableName = brokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    if (brokerMetrics != null) {
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED, numDocsScanned);
      brokerMetrics
          .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER, numEntriesScannedInFilter);
      brokerMetrics
          .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_POST_FILTER, numEntriesScannedPostFilter);

      if (numConsumingSegmentsProcessed > 0 && minConsumingFreshnessTimeMs > 0) {
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.FRESHNESS_LAG_MS,
            System.currentTimeMillis() - minConsumingFreshnessTimeMs, TimeUnit.MILLISECONDS);
      }
    }

    // Parse the option from request whether to preserve the type
    String preserveTypeString = (brokerRequest.getQueryOptions() == null) ? "false" : brokerRequest.getQueryOptions()
        .getOrDefault(CommonConstants.Broker.Request.QueryOptionKey.PRESERVE_TYPE, "false");
    boolean preserveType = Boolean.valueOf(preserveTypeString);

    if (dataTableMap.isEmpty()) {
      // For empty data table map, construct empty result using the cached data schema.

      // This will only happen to selection query.
      if (cachedDataSchema != null) {
        List<String> selectionColumns = SelectionOperatorUtils
            .getSelectionColumns(brokerRequest.getSelections().getSelectionColumns(), cachedDataSchema);
        brokerResponseNative.setSelectionResults(new SelectionResults(selectionColumns, new ArrayList<>(0)));
      }
    } else {
      // Reduce server responses data and set query results into the broker response.
      assert cachedDataSchema != null;

      if (brokerRequest.isSetSelections()) {
        // Selection query.

        // For data table map with more than one data tables, remove conflicting data tables.
        DataSchema masterDataSchema = cachedDataSchema.clone();
        if (dataTableMap.size() > 1) {
          List<String> droppedServers = removeConflictingResponses(masterDataSchema, dataTableMap);
          if (!droppedServers.isEmpty()) {
            String errorMessage =
                QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses for table: " + tableName
                    + " from servers: " + droppedServers + " got dropped due to data schema inconsistency.";
            LOGGER.info(errorMessage);
            if (brokerMetrics != null) {
              brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1L);
            }
            brokerResponseNative
                .addToExceptions(new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
          }
        }
        setSelectionResults(brokerResponseNative, brokerRequest.getSelections(), dataTableMap, masterDataSchema,
            preserveType);
      } else {
        // Aggregation query.
        AggregationFunction[] aggregationFunctions =
            AggregationFunctionUtils.getAggregationFunctions(brokerRequest.getAggregationsInfo());
        if (!brokerRequest.isSetGroupBy()) {
          // Aggregation only query.
          setAggregationResults(brokerResponseNative, aggregationFunctions, dataTableMap, cachedDataSchema,
              preserveType);
        } else {
          // Aggregation group-by query.
          boolean[] aggregationFunctionSelectStatus =
              AggregationFunctionUtils.getAggregationFunctionsSelectStatus(brokerRequest.getAggregationsInfo());
          setGroupByHavingResults(brokerResponseNative, aggregationFunctions, aggregationFunctionSelectStatus,
              brokerRequest.getGroupBy(), dataTableMap, brokerRequest.getHavingFilterQuery(),
              brokerRequest.getHavingFilterSubQueryMap(), preserveType);
          if (brokerMetrics != null && (!brokerResponseNative.getAggregationResults().isEmpty())) {
            // We emit the group by size when the result isn't empty. All the sizes among group-by results should be the same.
            // Thus, we can just emit the one from the 1st result.
            brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.GROUP_BY_SIZE,
                brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size());
          }
        }
      }
    }
    return brokerResponseNative;
  }

  /**
   * Given a data schema, remove data tables that are not compatible with this data schema.
   * <p>Upgrade the data schema passed in to cover all remaining data schemas.
   *
   * @param dataSchema data schema.
   * @param dataTableMap map from server to data table.
   * @return list of server names where the data table got removed.
   */
  @Nonnull
  private List<String> removeConflictingResponses(@Nonnull DataSchema dataSchema,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    List<String> droppedServers = new ArrayList<>();
    Iterator<Map.Entry<ServerInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerInstance, DataTable> entry = iterator.next();
      DataSchema dataSchemaToCompare = entry.getValue().getDataSchema();
      assert dataSchemaToCompare != null;
      if (!dataSchema.isTypeCompatibleWith(dataSchemaToCompare)) {
        droppedServers.add(entry.getKey().toString());
        iterator.remove();
      } else {
        dataSchema.upgradeToCover(dataSchemaToCompare);
      }
    }
    return droppedServers;
  }

  /**
   * Reduce selection results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param selection selection information.
   * @param dataTableMap map from server to data table.
   * @param dataSchema data schema.
   */
  private void setSelectionResults(@Nonnull BrokerResponseNative brokerResponseNative, @Nonnull Selection selection,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap, @Nonnull DataSchema dataSchema, boolean preserveType) {
    // Reduce the selection results.
    int selectionSize = selection.getSize();
    SelectionResults selectionResults;
    int[] columnIndices;
    List<String> selectionColumns =
        SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), dataSchema);
    if (selection.isSetSelectionSortSequence() && selectionSize != 0) {
      // Selection order-by.
      SelectionOperatorService selectionService = new SelectionOperatorService(selection, dataSchema);
      selectionService.reduceWithOrdering(dataTableMap);
      selectionResults = selectionService.renderSelectionResultsWithOrdering();
      columnIndices = SelectionOperatorUtils.getColumnIndices(selectionColumns, dataSchema);
    } else {
      // Selection only.
      selectionResults = SelectionOperatorUtils.renderSelectionResultsWithoutOrdering(
          SelectionOperatorUtils.reduceWithoutOrdering(dataTableMap, selectionSize), dataSchema, selectionColumns);
      columnIndices = SelectionOperatorUtils.getColumnIndices(selectionColumns, dataSchema);
    }

    // TODO: use "formatRowsWithoutOrdering", "formatRowsWithOrdering" properly for selection when the server is updated
    // to the latest code
    if (!preserveType) {
      selectionResults.setRows(
          SelectionOperatorUtils.formatRowsWithOrdering(selectionResults.getRows(), columnIndices, dataSchema));
    }

    brokerResponseNative.setSelectionResults(selectionResults);
  }

  /**
   * Reduce aggregation results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param aggregationFunctions array of aggregation functions.
   * @param dataTableMap map from server to data table.
   * @param dataSchema data schema.
   */
  @SuppressWarnings("unchecked")
  private void setAggregationResults(@Nonnull BrokerResponseNative brokerResponseNative,
      @Nonnull AggregationFunction[] aggregationFunctions, @Nonnull Map<ServerInstance, DataTable> dataTableMap,
      @Nonnull DataSchema dataSchema, boolean preserveType) {
    int numAggregationFunctions = aggregationFunctions.length;

    // Merge results from all data tables.
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : dataTableMap.values()) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediateResultToMerge;
        DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
        switch (columnDataType) {
          case LONG:
            intermediateResultToMerge = dataTable.getLong(0, i);
            break;
          case DOUBLE:
            intermediateResultToMerge = dataTable.getDouble(0, i);
            break;
          case OBJECT:
            intermediateResultToMerge = dataTable.getObject(0, i);
            break;
          default:
            throw new IllegalStateException("Illegal column data type in aggregation results: " + columnDataType);
        }
        Object mergedIntermediateResult = intermediateResults[i];
        if (mergedIntermediateResult == null) {
          intermediateResults[i] = intermediateResultToMerge;
        } else {
          intermediateResults[i] = aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
        }
      }
    }

    // Extract final results and set them into the broker response.
    List<AggregationResult> reducedAggregationResults = new ArrayList<>(numAggregationFunctions);
    for (int i = 0; i < numAggregationFunctions; i++) {
      Serializable resultValue = AggregationFunctionUtils
          .getSerializableValue(aggregationFunctions[i].extractFinalResult(intermediateResults[i]));

      // Format the value into string if required
      if (!preserveType) {
        resultValue = AggregationFunctionUtils.formatValue(resultValue);
      }
      reducedAggregationResults.add(new AggregationResult(dataSchema.getColumnName(i), resultValue));
    }
    brokerResponseNative.setAggregationResults(reducedAggregationResults);
  }

  /**
   * Reduce group-by results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param aggregationFunctions array of aggregation functions.
   * @param groupBy group-by information.
   * @param dataTableMap map from server to data table.
   * @param havingFilterQuery having filter query
   * @param havingFilterQueryMap having filter query map
   */
  @SuppressWarnings("unchecked")
  private void setGroupByHavingResults(@Nonnull BrokerResponseNative brokerResponseNative,
      @Nonnull AggregationFunction[] aggregationFunctions, boolean[] aggregationFunctionsSelectStatus,
      @Nonnull GroupBy groupBy, @Nonnull Map<ServerInstance, DataTable> dataTableMap,
      HavingFilterQuery havingFilterQuery, HavingFilterQueryMap havingFilterQueryMap, boolean preserveType) {
    int numAggregationFunctions = aggregationFunctions.length;

    // Merge results from all data tables.
    String[] columnNames = new String[numAggregationFunctions];
    Map<String, Object>[] intermediateResultMaps = new Map[numAggregationFunctions];
    for (DataTable dataTable : dataTableMap.values()) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        if (columnNames[i] == null) {
          columnNames[i] = dataTable.getString(i, 0);
          intermediateResultMaps[i] = dataTable.getObject(i, 1);
        } else {
          Map<String, Object> mergedIntermediateResultMap = intermediateResultMaps[i];
          Map<String, Object> intermediateResultMapToMerge = dataTable.getObject(i, 1);
          for (Map.Entry<String, Object> entry : intermediateResultMapToMerge.entrySet()) {
            String groupKey = entry.getKey();
            Object intermediateResultToMerge = entry.getValue();
            if (mergedIntermediateResultMap.containsKey(groupKey)) {
              Object mergedIntermediateResult = mergedIntermediateResultMap.get(groupKey);
              mergedIntermediateResultMap
                  .put(groupKey, aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge));
            } else {
              mergedIntermediateResultMap.put(groupKey, intermediateResultToMerge);
            }
          }
        }
      }
    }

    // Extract final result maps from the merged intermediate result maps.
    Map<String, Comparable>[] finalResultMaps = new Map[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      Map<String, Object> intermediateResultMap = intermediateResultMaps[i];
      Map<String, Comparable> finalResultMap = new HashMap<>();
      for (String groupKey : intermediateResultMap.keySet()) {
        Object intermediateResult = intermediateResultMap.get(groupKey);
        finalResultMap.put(groupKey, aggregationFunctions[i].extractFinalResult(intermediateResult));
      }
      finalResultMaps[i] = finalResultMap;
    }
    //If HAVING clause is set, we further filter the group by results based on the HAVING predicate
    if (havingFilterQuery != null) {
      HavingClauseComparisonTree havingClauseComparisonTree =
          HavingClauseComparisonTree.buildHavingClauseComparisonTree(havingFilterQuery, havingFilterQueryMap);
      //Applying close policy
      //We just keep those groups (from different aggregation functions) that are exist in the result set of all aggregation functions.
      //In other words, we just keep intersection of groups of different aggregation functions.
      //Here we calculate the intersection of group key sets of different aggregation functions
      Set<String> intersectionOfKeySets = finalResultMaps[0].keySet();
      for (int i = 1; i < numAggregationFunctions; i++) {
        intersectionOfKeySets.retainAll(finalResultMaps[i].keySet());
      }

      //Now it is time to remove those groups that do not validate HAVING clause predicate
      //We use TreeMap which supports CASE_INSENSITIVE_ORDER
      Map<String, Comparable> singleGroupAggResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      Map<String, Comparable>[] finalFilteredResultMaps = new Map[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        finalFilteredResultMaps[i] = new HashMap<>();
      }

      for (String groupKey : intersectionOfKeySets) {
        for (int i = 0; i < numAggregationFunctions; i++) {
          singleGroupAggResults.put(columnNames[i], finalResultMaps[i].get(groupKey));
        }
        //if this group validate HAVING predicate keep it in the new map
        if (havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults)) {
          for (int i = 0; i < numAggregationFunctions; i++) {
            finalFilteredResultMaps[i].put(groupKey, singleGroupAggResults.get(columnNames[i]));
          }
        }
      }
      //update the final results
      finalResultMaps = finalFilteredResultMaps;
    }

    int aggregationNumsInFinalResult = 0;
    for (int i = 0; i < numAggregationFunctions; i++) {
      if (aggregationFunctionsSelectStatus[i]) {
        aggregationNumsInFinalResult++;
      }
    }

    if (aggregationNumsInFinalResult > 0) {
      String[] finalColumnNames = new String[aggregationNumsInFinalResult];
      Map<String, Comparable>[] finalOutResultMaps = new Map[aggregationNumsInFinalResult];
      AggregationFunction[] finalAggregationFunctions = new AggregationFunction[aggregationNumsInFinalResult];
      int count = 0;
      for (int i = 0; i < numAggregationFunctions; i++) {
        if (aggregationFunctionsSelectStatus[i]) {
          finalColumnNames[count] = columnNames[i];
          finalOutResultMaps[count] = finalResultMaps[i];
          finalAggregationFunctions[count] = aggregationFunctions[i];
          count++;
        }
      }
      // Trim the final result maps to topN and set them into the broker response.
      AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
          new AggregationGroupByTrimmingService(finalAggregationFunctions, (int) groupBy.getTopN());
      List<GroupByResult>[] groupByResultLists = aggregationGroupByTrimmingService.trimFinalResults(finalOutResultMaps);

      // Format the value into string if required
      if (!preserveType) {
        for (List<GroupByResult> groupByResultList : groupByResultLists) {
          for (GroupByResult groupByResult : groupByResultList) {
            groupByResult.setValue(AggregationFunctionUtils.formatValue(groupByResult.getValue()));
          }
        }
      }

      List<AggregationResult> aggregationResults = new ArrayList<>(count);
      for (int i = 0; i < aggregationNumsInFinalResult; i++) {
        List<GroupByResult> groupByResultList = groupByResultLists[i];
        aggregationResults.add(new AggregationResult(groupByResultList, groupBy.getExpressions(), finalColumnNames[i]));
      }
      brokerResponseNative.setAggregationResults(aggregationResults);
    } else {
      throw new IllegalStateException(
          "There should be minimum one aggregation function in the select list of a Group by query");
    }
  }
}
