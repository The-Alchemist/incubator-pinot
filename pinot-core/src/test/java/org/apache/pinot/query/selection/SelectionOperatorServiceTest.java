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
package org.apache.pinot.query.selection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.selection.SelectionOperatorService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * The <code>SelectionOperatorServiceTest</code> class provides unit tests for {@link SelectionOperatorUtils} and
 * {@link SelectionOperatorService}.
 */
public class SelectionOperatorServiceTest {
  private final String[] _columnNames =
      {"int", "long", "float", "double", "string", "int_array", "long_array", "float_array", "double_array", "string_array"};
  private final DataSchema.ColumnDataType[] _columnDataTypes =
      {DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.FLOAT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY};
  private final DataSchema _dataSchema = new DataSchema(_columnNames, _columnDataTypes);
  private final DataSchema.ColumnDataType[] _compatibleColumnDataTypes =
      {DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.FLOAT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY};
  private final DataSchema _compatibleDataSchema = new DataSchema(_columnNames, _compatibleColumnDataTypes);
  private final DataSchema.ColumnDataType[] _upgradedColumnDataTypes =
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY};
  private final DataSchema _upgradedDataSchema = new DataSchema(_columnNames, _upgradedColumnDataTypes);
  private final Serializable[] _row1 =
      {0, 1L, 2.0F, 3.0, "4", new int[]{5}, new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new String[]{"9"}};
  private final Serializable[] _row2 =
      {10, 11L, 12.0F, 13.0, "14", new int[]{15}, new long[]{16L}, new float[]{17.0F}, new double[]{18.0}, new String[]{"19"}};
  private final Serializable[] _compatibleRow1 =
      {1L, 2.0F, 3.0, 4, "5", new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new int[]{9}, new String[]{"10"}};
  private final Serializable[] _compatibleRow2 =
      {11L, 12.0F, 13.0, 14, "15", new long[]{16L}, new float[]{17.0F}, new double[]{18.0}, new int[]{19}, new String[]{"20"}};
  private final Selection _selectionOrderBy = new Selection();

  @BeforeClass
  public void setUp() {
    // SELECT * FROM table ORDER BY int DESC LIMIT 1, 2.
    _selectionOrderBy.setSelectionColumns(Arrays.asList(_columnNames));
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("int");
    selectionSort.setIsAsc(false);
    _selectionOrderBy.setSelectionSortSequence(Collections.singletonList(selectionSort));
    _selectionOrderBy.setSize(2);
    _selectionOrderBy.setOffset(1);
  }

  @Test
  public void testGetSelectionColumns() {
    List<String> selectionColumns =
        SelectionOperatorUtils.getSelectionColumns(Collections.singletonList("*"), _dataSchema);
    // Alphabetical.
    List<String> expectedSelectionColumns = Arrays
        .asList("double", "double_array", "float", "float_array", "int", "int_array", "long", "long_array", "string",
            "string_array");
    Assert.assertEquals(selectionColumns, expectedSelectionColumns);
  }

  @Test
  public void testCompatibleRowsMergeWithoutOrdering() {
    ArrayList<Serializable[]> mergedRows = new ArrayList<>(2);
    mergedRows.add(_row1.clone());
    mergedRows.add(_row2.clone());
    Collection<Serializable[]> rowsToMerge = new ArrayList<>(2);
    rowsToMerge.add(_compatibleRow1.clone());
    rowsToMerge.add(_compatibleRow2.clone());
    SelectionOperatorUtils.mergeWithoutOrdering(mergedRows, rowsToMerge, 3);
    Assert.assertEquals(mergedRows.size(), 3);
    Assert.assertEquals(mergedRows.get(0), _row1);
    Assert.assertEquals(mergedRows.get(1), _row2);
    Assert.assertEquals(mergedRows.get(2), _compatibleRow1);
  }

  @Test
  public void testCompatibleRowsMergeWithOrdering() {
    SelectionOperatorService selectionOperatorService = new SelectionOperatorService(_selectionOrderBy, _dataSchema);
    PriorityQueue<Serializable[]> mergedRows = selectionOperatorService.getRows();
    Collection<Serializable[]> rowsToMerge1 = new ArrayList<>(2);
    rowsToMerge1.add(_row1.clone());
    rowsToMerge1.add(_row2.clone());
    Collection<Serializable[]> rowsToMerge2 = new ArrayList<>(2);
    rowsToMerge2.add(_compatibleRow1.clone());
    rowsToMerge2.add(_compatibleRow2.clone());
    int maxNumRows = _selectionOrderBy.getOffset() + _selectionOrderBy.getSize();
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge1, maxNumRows);
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge2, maxNumRows);
    Assert.assertEquals(mergedRows.size(), 3);
    Assert.assertEquals(mergedRows.poll(), _compatibleRow1);
    Assert.assertEquals(mergedRows.poll(), _row2);
    Assert.assertEquals(mergedRows.poll(), _compatibleRow2);
  }

  @Test
  public void testCompatibleRowsDataTableTransformation()
      throws Exception {
    Collection<Serializable[]> rows = new ArrayList<>(2);
    rows.add(_row1.clone());
    rows.add(_compatibleRow1.clone());
    DataSchema dataSchema = _dataSchema.clone();
    Assert.assertTrue(dataSchema.isTypeCompatibleWith(_compatibleDataSchema));
    dataSchema.upgradeToCover(_compatibleDataSchema);
    Assert.assertEquals(dataSchema, _upgradedDataSchema);
    DataTable dataTable = SelectionOperatorUtils.getDataTableFromRows(rows, dataSchema);
    Serializable[] expectedRow1 =
        {0L, 1.0, 2.0, 3.0, "4", new long[]{5L}, new double[]{6.0}, new double[]{7.0}, new double[]{8.0}, new String[]{"9"}};
    Serializable[] expectedCompatibleRow1 =
        {1L, 2.0, 3.0, 4.0, "5", new long[]{6L}, new double[]{7.0}, new double[]{8.0}, new double[]{9.0}, new String[]{"10"}};
    Assert.assertTrue(Arrays.deepEquals(SelectionOperatorUtils.extractRowFromDataTable(dataTable, 0), expectedRow1));
    Assert.assertTrue(
        Arrays.deepEquals(SelectionOperatorUtils.extractRowFromDataTable(dataTable, 1), expectedCompatibleRow1));
  }

  @Test
  public void testCompatibleRowsRenderSelectionResultsWithoutOrdering() {
    List<Serializable[]> rows = new ArrayList<>(2);
    rows.add(_row1.clone());
    rows.add(_compatibleRow1.clone());
    SelectionResults selectionResults = SelectionOperatorUtils
        .renderSelectionResultsWithoutOrdering(rows, _upgradedDataSchema, Arrays.asList(_columnNames));

    List<Serializable[]> resultRows = selectionResults.getRows();

    Serializable[] expectedRow1 =
        {0, 1L, 2.0F, 3.0, "4", new int[]{5}, new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new String[]{"9"}};
    Serializable[] expectedRow2 =
        {1L, 2.0F, 3.0, 4, "5", new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new int[]{9}, new String[]{"10"}};

    Assert.assertTrue(Arrays.deepEquals(resultRows.get(0), expectedRow1));
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(1), expectedRow2));

    int[] columnIndices =
        SelectionOperatorUtils.getColumnIndices(selectionResults.getColumns(), _dataSchema);

    // TODO: use "formatRowsWithoutOrdering" after server updated to the latest code.
    resultRows = SelectionOperatorUtils.formatRowsWithOrdering(resultRows, columnIndices, _upgradedDataSchema);

    Serializable[] expectedFormattedRow1 =
        {"0", "1.0", "2.0", "3.0", "4", new String[]{"5"}, new String[]{"6.0"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9"}};
    Serializable[] expectedFormattedRow2 =
        {"1", "2.0", "3.0", "4.0", "5", new String[]{"6"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9.0"}, new String[]{"10"}};
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(0), expectedFormattedRow1));
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(1), expectedFormattedRow2));
  }

  @Test
  public void testCompatibleRowsRenderSelectionResultsWithOrdering() {
    SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(_selectionOrderBy, _upgradedDataSchema);
    PriorityQueue<Serializable[]> rows = selectionOperatorService.getRows();
    rows.offer(_row1.clone());
    rows.offer(_compatibleRow1.clone());
    rows.offer(_compatibleRow2.clone());

    SelectionResults selectionResults = selectionOperatorService.renderSelectionResultsWithOrdering();
    List<Serializable[]> resultRows = selectionResults.getRows();

    Serializable[] expectedRow1 =
        {1L, 2.0F, 3.0, 4, "5", new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new int[]{9}, new String[]{"10"}};
    Serializable[] expectedRow2 =
        {0, 1L, 2.0F, 3.0, "4", new int[]{5}, new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new String[]{"9"}};
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(0), expectedRow1));
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(1), expectedRow2));

    int[] columnIndices =
        SelectionOperatorUtils.getColumnIndices(selectionResults.getColumns(), _dataSchema);
    resultRows = SelectionOperatorUtils.formatRowsWithOrdering(resultRows, columnIndices, _upgradedDataSchema);

    Serializable[] expectedFormattedRow1 =
        {"1", "2.0", "3.0", "4.0", "5", new String[]{"6"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9.0"}, new String[]{"10"}};
    Serializable[] expectedFormattedRow2 =
        {"0", "1.0", "2.0", "3.0", "4", new String[]{"5"}, new String[]{"6.0"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9"}};
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(0), expectedFormattedRow1));
    Assert.assertTrue(Arrays.deepEquals(resultRows.get(1), expectedFormattedRow2));
  }
}
