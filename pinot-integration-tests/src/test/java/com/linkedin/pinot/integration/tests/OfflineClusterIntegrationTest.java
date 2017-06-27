/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class OfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 3;
  private static final int NUM_SERVERS = 5;
  private static final long MAX_RELOAD_TIME_IN_MILLIS = 60_000L;

  // For inverted index triggering test
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS =
      Arrays.asList("FlightNum", "Origin", "Quarter", "DivActualElapsedTime");
  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";

  // For default columns test
  private static final String SCHEMA_WITH_EXTRA_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_extra_columns.schema";
  private static final String SCHEMA_WITH_MISSING_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_missing_columns.schema";
  private static final String TEST_DEFAULT_COLUMNS_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable";

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(NUM_BROKERS + NUM_SERVERS);

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(NUM_BROKERS);
    startServers(NUM_SERVERS);

    // Set up service status callbacks
    List<String> instances = _helixAdmin.getInstancesInCluster(_clusterName);
    for (String instance : instances) {
      if (instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        _serviceStatusCallbacks.add(
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _clusterName, instance,
                Collections.singletonList(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)));
      }
      if (instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        _serviceStatusCallbacks.add(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
            new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, _clusterName,
                instance),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _clusterName,
                instance))));
      }
    }

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create segments from Avro data
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, getTableName(), false,
        getRawIndexColumns(), null, executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create the table
    addOfflineTable("DaysSinceEpoch", "daysSinceEpoch", -1, null, null, null, getInvertedIndexColumns(), getLoadMode(),
        getTableName(), SegmentVersion.v1, getTaskConfig());

    // Upload all segments
    for (String segmentName : _tarDir.list()) {
      File segmentFile = new File(_tarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, segmentFile, segmentFile.length());
    }

    // Wait for all documents loaded
    waitForAllDocsLoaded(60_000L);
  }

  @Test
  @Override
  public void testQueriesFromQueryFile()
      throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  @Override
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    super.testGeneratedQueriesWithMultiValues();
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    super.testInstanceShutdown();
  }

  @Test
  public void testInstancesStarted() {
    Assert.assertEquals(_serviceStatusCallbacks.size(), NUM_BROKERS + NUM_SERVERS);
    for (ServiceStatus.ServiceStatusCallback serviceStatusCallback : _serviceStatusCallbacks) {
      Assert.assertEquals(serviceStatusCallback.getServiceStatus(), ServiceStatus.Status.GOOD);
    }
  }

  @Test
  public void testInvertedIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    JSONObject queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    Assert.assertEquals(queryResponse.getLong("numEntriesScannedInFilter"), numTotalDocs);
    updateOfflineTable("DaysSinceEpoch", -1, "", null, null, UPDATED_INVERTED_INDEX_COLUMNS, getLoadMode(),
        getTableName(), SegmentVersion.v1, getTaskConfig());

    // Trigger reload
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable/segments/reload?type=offline");

    long endTime = System.currentTimeMillis() + MAX_RELOAD_TIME_IN_MILLIS;
    while (System.currentTimeMillis() < endTime) {
      // Query 20 times to ensure all replicas get reloaded
      boolean allReplicasReloaded = true;
      for (int i = 0; i < 20; i++) {
        queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        Assert.assertEquals(queryResponse.getLong("totalDocs"), numTotalDocs);
        if (queryResponse.getLong("numEntriesScannedInFilter") != 0L) {
          allReplicasReloaded = false;
          break;
        }
      }
      if (allReplicasReloaded) {
        return;
      }
      Thread.sleep(1000L);
    }
    Assert.fail("Failed to generate inverted index in " + MAX_RELOAD_TIME_IN_MILLIS + "ms");
  }

  /**
   * We will add extra new columns to the schema to test adding new columns with default value to the offline segments.
   * <p>New columns are: (name, field type, data type, single/multi value, default null value)
   * <ul>
   *   <li>"newAddedIntMetric", METRIC, INT, single-value, 1</li>
   *   <li>"newAddedLongMetric", METRIC, LONG, single-value, 1</li>
   *   <li>"newAddedFloatMetric", METRIC, FLOAT, single-value, default (0.0)</li>
   *   <li>"newAddedDoubleMetric", METRIC, DOUBLE, single-value, default (0.0)</li>
   *   <li>"newAddedIntDimension", DIMENSION, INT, single-value, default (Integer.MIN_VALUE)</li>
   *   <li>"newAddedLongDimension", DIMENSION, LONG, single-value, default (Long.MIN_VALUE)</li>
   *   <li>"newAddedFloatDimension", DIMENSION, FLOAT, single-value, default (Float.NEGATIVE_INFINITY)</li>
   *   <li>"newAddedDoubleDimension", DIMENSION, DOUBLE, single-value, default (Double.NEGATIVE_INFINITY)</li>
   *   <li>"newAddedStringDimension", DIMENSION, STRING, multi-value, "newAdded"</li>
   * </ul>
   */
  @Test
  public void testDefaultColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    reloadDefaultColumns(true);
    JSONObject queryResponse = postQuery(SELECT_STAR_QUERY);
    Assert.assertEquals(queryResponse.getLong("totalDocs"), numTotalDocs);
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 88);

    testNewAddedColumns();

    reloadDefaultColumns(false);
    queryResponse = postQuery(SELECT_STAR_QUERY);
    Assert.assertEquals(queryResponse.getLong("totalDocs"), numTotalDocs);
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 79);
  }

  private void reloadDefaultColumns(boolean withExtraColumns)
      throws Exception {
    long numTotalDocs = getCountStarResult();

    if (withExtraColumns) {
      sendSchema(SCHEMA_WITH_EXTRA_COLUMNS);
    } else {
      sendSchema(SCHEMA_WITH_MISSING_COLUMNS);
    }

    // Trigger reload
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable/segments/reload?type=offline");

    long endTime = System.currentTimeMillis() + MAX_RELOAD_TIME_IN_MILLIS;
    while (System.currentTimeMillis() < endTime) {
      // Query 20 times to ensure all replicas get reloaded
      boolean allReplicasReloaded = true;
      for (int i = 0; i < 20; i++) {
        JSONObject queryResponse = postQuery(TEST_DEFAULT_COLUMNS_QUERY);
        // Total docs should not change during reload
        Assert.assertEquals(queryResponse.getLong("totalDocs"), numTotalDocs);
        long count = queryResponse.getJSONArray("aggregationResults").getJSONObject(0).getLong("value");
        if (withExtraColumns) {
          if (count != numTotalDocs) {
            allReplicasReloaded = false;
            break;
          }
        } else {
          if (count != 0) {
            allReplicasReloaded = false;
            break;
          }
        }
      }
      if (allReplicasReloaded) {
        return;
      }
      Thread.sleep(1000L);
    }
    if (withExtraColumns) {
      Assert.fail("Failed to add default columns in " + MAX_RELOAD_TIME_IN_MILLIS + "ms");
    } else {
      Assert.fail("Failed to remove default columns in " + MAX_RELOAD_TIME_IN_MILLIS + "ms");
    }
  }

  private void sendSchema(String resourceName)
      throws Exception {
    URL resource = OfflineClusterIntegrationTest.class.getClassLoader().getResource(resourceName);
    Assert.assertNotNull(resource);
    File schemaFile = new File(resource.getFile());
    addSchema(schemaFile, getTableName());
  }

  private void testNewAddedColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    String pqlQuery;
    String sqlQuery;

    // Test queries with each new added columns
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntMetric = 1";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongMetric = 1";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatMetric = 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleMetric = 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongDimension < 0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatDimension < 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleDimension < 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedStringDimension = 'newAdded'";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test queries with new added metric column in aggregation function
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test other query forms with new added columns
    JSONObject response;
    JSONObject groupByResult;
    pqlQuery = "SELECT SUM(NewAddedFloatMetric) FROM mytable GROUP BY NewAddedStringDimension";
    response = postQuery(pqlQuery);
    groupByResult =
        response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), "newAdded");
    pqlQuery = "SELECT SUM(NewAddedDoubleMetric) FROM mytable GROUP BY NewAddedIntDimension";
    response = postQuery(pqlQuery);
    groupByResult =
        response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable GROUP BY NewAddedLongDimension";
    response = postQuery(pqlQuery);
    groupByResult =
        response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), numTotalDocs);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Long.MIN_VALUE));
    pqlQuery =
        "SELECT SUM(NewAddedIntMetric), SUM(NewAddedLongMetric), SUM(NewAddedFloatMetric), SUM(NewAddedDoubleMetric) "
            + "FROM mytable GROUP BY NewAddedIntDimension, NewAddedLongDimension, NewAddedFloatDimension, "
            + "NewAddedDoubleDimension, NewAddedStringDimension";
    response = postQuery(pqlQuery);
    JSONArray groupByResultArray = response.getJSONArray("aggregationResults");
    groupByResult = groupByResultArray.getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), numTotalDocs);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    groupByResult = groupByResultArray.getJSONObject(1).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), numTotalDocs);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    groupByResult = groupByResultArray.getJSONObject(2).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    groupByResult = groupByResultArray.getJSONObject(3).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Test instance decommission before tearing down
    testInstanceDecommission();

    // Brokers and servers has been stopped
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void testInstanceDecommission()
      throws Exception {
    ControllerRequestURLBuilder requestBuilder = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);

    // Fetch all instances
    JSONObject response = new JSONObject(sendGetRequest(requestBuilder.forInstanceList()));
    JSONArray instanceList = response.getJSONArray("instances");
    int numInstances = instanceList.length();
    assertEquals(numInstances, NUM_BROKERS + NUM_SERVERS);

    // Try to delete a server that does not exist
    String deleteInstanceRequest = requestBuilder.forInstanceDelete("potato");
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (404)");
    } catch (IOException e) {
      // Expected exception on 404 status code
    }

    // Get the server name
    String serverName = null;
    String brokerName = null;
    for (int i = 0; i < numInstances; i++) {
      String instanceName = instanceList.getString(i);
      if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        serverName = instanceName;
      } else if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        brokerName = instanceName;
      }
    }

    // Try to delete a live server
    deleteInstanceRequest = requestBuilder.forInstanceDelete(serverName);
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Stop servers
    stopServer();

    // Try to delete a server whose information is still on the ideal state
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Delete the table
    dropOfflineTable(getTableName());

    // Now, delete server should work
    response = new JSONObject(sendDeleteRequest(deleteInstanceRequest));
    Assert.assertEquals(response.getString("status"), "success");

    // Try to delete a broker whose information is still live
    try {
      deleteInstanceRequest = requestBuilder.forInstanceDelete(brokerName);
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Stop brokers
    stopBroker();

    // TODO: Add test to delete broker instance. Currently, stopBroker() does not work correctly.

    // Check if '/INSTANCES/<serverName>' has been erased correctly
    String instancePath = "/" + _clusterName + "/INSTANCES/" + serverName;
    Assert.assertFalse(_propertyStore.exists(instancePath, 0));

    // Check if '/CONFIGS/PARTICIPANT/<serverName>' has been erased correctly
    String configPath = "/" + _clusterName + "/CONFIGS/PARTICIPANT/" + serverName;
    Assert.assertFalse(_propertyStore.exists(configPath, 0));
  }
}
