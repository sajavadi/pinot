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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.tools.query.comparison.QueryComparison;
import com.linkedin.pinot.tools.scan.query.QueryResponse;
import com.linkedin.pinot.tools.scan.query.ScanBasedQueryProcessor;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Cluster integration test that compares with the scan based comparison tool. It takes at least three Avro files and
 * creates offline segments for the first two, while it pushes all Avro files except the first one into Kafka.
 *
 * Each Avro file is pushed in order into Kafka, and after each Avro file is pushed, we run all queries from the query
 * source given when the record count is stabilized. This makes it so that we cover as many test conditions as possible
 * (eg. partially in memory, partially on disk with data overlap between offline and realtime) and should give us good
 * confidence that the cluster behaves as it should.
 *
 * Because this test is configurable, it can also be used with arbitrary Avro files (within reason) and query logs from
 * production to reproduce issues seen in the offline-realtime interaction in a controlled environment.
 *
 * To reuse this test, you need to implement getAvroFileCount, getAllAvroFiles, getOfflineAvroFiles and
 * getRealtimeAvroFiles, as well as the optional extractAvroIfNeeded. You'll also need to use one of the test methods.
 */
// TODO: clean up this test
public abstract class HybridClusterScanComparisonIntegrationTest extends HybridClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridClusterScanComparisonIntegrationTest.class);

  protected final File _offlineTarDir = new File(_tempDir, "offlineTarDir");
  protected final File _realtimeTarDir = new File(_tempDir, "realtimeTarDir");
  protected final File _unpackedSegments = new File(_tempDir, "unpackedSegments");
  protected final File _offlineSegmentDir = new File(_segmentDir, "offline");
  protected final File _realtimeSegmentDir = new File(_segmentDir, "realtime");
  protected final String _kafkaTopic = getKafkaTopic();
  private Map<File, File> _offlineAvroToSegmentMap;
  private Map<File, File> _realtimeAvroToSegmentMap;
  private File _schemaFile;
  private Schema _schema;
  protected ScanBasedQueryProcessor _scanBasedQueryProcessor;
  private FileWriter _compareStatusFileWriter;
  private FileWriter _scanRspFileWriter;
  private final AtomicInteger _successfulQueries = new AtomicInteger(0);
  private final AtomicInteger _failedQueries = new AtomicInteger(0);
  private final AtomicInteger _emptyResults = new AtomicInteger(0);
  private long startTimeMs;
  private boolean _logMatchingResults = false;
  private ThreadPoolExecutor _queryExecutor;
  private long _countStarResult;
  protected List<String> invertedIndexColumns = new ArrayList<String>();
  protected boolean _createSegmentsInParallel = false;
  protected int _nQueriesRead = -1;

  @AfterClass
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    _compareStatusFileWriter.write("\nSuccessful queries: " + _successfulQueries.get() + "\n" +
        "Failed queries: " + _failedQueries.get() + "\n" +
        "Empty results: " + _emptyResults.get() + "\n");
    _compareStatusFileWriter.write("Finish time:" + System.currentTimeMillis() + "\n");
    _compareStatusFileWriter.close();

    if (_failedQueries.get() != 0) {
      Assert.fail("More than one query failed to compare properly, see the log file.");
    }
  }

  @Override
  protected void cleanup() throws Exception {
    // Comment this to preserve segments for examination later
    super.cleanup();
  }

  protected abstract String getTimeColumnName();

  protected abstract String getTimeColumnType();

  protected abstract String getSortedColumn();

  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile, String sortedColumn, List<String> invertedIndexColumns)
      throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    addHybridTable(tableName, timeColumnName, timeColumnType, kafkaZkUrl, kafkaTopic, schema.getSchemaName(),
        "TestTenant", "TestTenant", avroFile, sortedColumn, invertedIndexColumns, "MMAP", useLlc(), null, null);
  }

  @Override
  @BeforeClass
  public void setUp() throws Exception {
    //Clean up
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _offlineSegmentDir, _realtimeSegmentDir,
        _offlineTarDir, _realtimeTarDir, _unpackedSegments);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    extractAvroIfNeeded();

    List<File> avroFiles = getAllAvroFiles();
    Assert.assertTrue(avroFiles.size() >= 3, "Need at least three Avro files for this test");

    _schemaFile = getSchemaFile();
    _schema = Schema.fromFile(_schemaFile);

    // Create Pinot table
    setUpTable("mytable", getTimeColumnName(), getTimeColumnType(), KafkaStarterUtils.DEFAULT_ZK_STR, _kafkaTopic,
        _schemaFile, avroFiles.get(0), getSortedColumn(), invertedIndexColumns);

    final List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles);
    final List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles);

    // Create segments from Avro data
    ExecutorService executor;
    if (_createSegmentsInParallel) {
      executor = Executors.newCachedThreadPool();
    } else {
      executor = Executors.newSingleThreadExecutor();
    }
    Future<Map<File, File>> offlineAvroToSegmentMapFuture =
        ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, 0, _offlineSegmentDir,
            _offlineTarDir, "mytable", false, null, _schema, executor);
    Future<Map<File, File>> realtimeAvroToSegmentMapFuture =
        ClusterIntegrationTestUtils.buildSegmentsFromAvro(realtimeAvroFiles, 0, _realtimeSegmentDir,
            _realtimeTarDir, "mytable", false, null, _schema, executor);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    // Redeem futures
    _offlineAvroToSegmentMap = offlineAvroToSegmentMapFuture.get();
    _realtimeAvroToSegmentMap = realtimeAvroToSegmentMapFuture.get();

    LOGGER.info("Offline avro to segment map: {}", _offlineAvroToSegmentMap);
    LOGGER.info("Realtime avro to segment map: {}", _realtimeAvroToSegmentMap);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Upload the offline segments
    int i = 0;
    for (String segmentName : _offlineTarDir.list()) {
      i++;
      LOGGER.info("Uploading segment {} : {}", i, segmentName);
      File file = new File(_offlineTarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, file, file.length());
    }

    waitForAllSegmentsOnline(CommonConstants.Helix.TableType.OFFLINE, getNumOfflineSegments(), 60_000L);

    _compareStatusFileWriter = getLogWriter();
    _scanRspFileWriter = getScanRspRecordFileWriter();
    _compareStatusFileWriter.write("Start time:" + System.currentTimeMillis() + "\n");
    _compareStatusFileWriter.flush();
    startTimeMs = System.currentTimeMillis();
    LOGGER.info("Setup completed");
  }

  protected void extractAvroIfNeeded() throws IOException {
    // Do nothing.
  }

  /**
   * The Avro files must be sorted as time series data.
   *
   * @return List of Avro files.
   */
  protected abstract List<File> getAllAvroFiles();

  @Override
  protected long getCountStarResult() {
    return _countStarResult;
  }

  @Override
  protected int getNumOfflineSegments() {
    return 2;
  }

  @Override
  protected int getNumRealtimeSegments() {
    return getAllAvroFiles().size() - 1;
  }

  protected void setLogMatchingResults(boolean logMatchingResults) {
    _logMatchingResults = logMatchingResults;
  }

  protected FileWriter getLogWriter() throws IOException {
    return new FileWriter("failed-queries-hybrid.log");
  }

  protected FileWriter getScanRspRecordFileWriter() {
    return null;
  }

  protected void runQueryAsync(final String pqlQuery, final String scanResult) {
    _queryExecutor.execute(new Runnable() {
      @Override
      public void run() {
        final ScanBasedQueryProcessor scanBasedQueryProcessor =
            (new ThreadLocal<ScanBasedQueryProcessor>() {
              @Override
              public ScanBasedQueryProcessor get() {
                return _scanBasedQueryProcessor.clone();
              }
            }).get();

        try {
          runQuery(pqlQuery, scanBasedQueryProcessor, false, scanResult);
        } catch (Exception e) {
          Assert.fail("Caught exception", e);
        }
      }
    });
  }

  protected long getStabilizationTimeMs() {
    return 120000L;
  }

  protected void runQuery(String pqlQuery, ScanBasedQueryProcessor scanBasedQueryProcessor, boolean displayStatus, String scanResult)
      throws Exception {
    JSONObject scanJson;
    if (scanResult == null) {
      QueryResponse scanResponse = scanBasedQueryProcessor.processQuery(pqlQuery);
      String scanRspStr = new ObjectMapper().writeValueAsString(scanResponse);
      if (_scanRspFileWriter != null) {
        if (scanRspStr.contains("\n")) {
          throw new RuntimeException("We don't handle new lines in json responses yet. The reader will parse newline as separator between query responses");
        }
        _scanRspFileWriter.write(scanRspStr + "\n");
      }
      scanJson = new JSONObject(scanRspStr);
    } else {
      scanJson = new JSONObject(scanResult);
    }
    JSONObject pinotJson = postQuery(pqlQuery);
    QueryComparison.setCompareNumDocs(false);

    try {
      QueryComparison.ComparisonStatus comparisonStatus = QueryComparison.compareWithEmpty(pinotJson, scanJson);

      if (comparisonStatus.equals(QueryComparison.ComparisonStatus.FAILED)) {
        _compareStatusFileWriter.write("\nQuery comparison failed for query " + _nQueriesRead + ":" + pqlQuery + "\n" +
            "Scan json: " + scanJson + "\n" +
            "Pinot json: " + pinotJson + "\n");
        _failedQueries.getAndIncrement();
      } else {
        _successfulQueries.getAndIncrement();
        if (comparisonStatus.equals(QueryComparison.ComparisonStatus.EMPTY) ) {
          _emptyResults.getAndIncrement();
        } else if (_logMatchingResults) {
          _compareStatusFileWriter.write("\nMatched for query:" + pqlQuery + "\n" + scanJson + "\n");
        }
      }
      _compareStatusFileWriter.flush();
    } catch (Exception e) {
      _compareStatusFileWriter.write("Caught exception while running query comparison, failed for query " + pqlQuery + "\n" +
          "Scan json: " + scanJson + "\n" +
          "Pinot json: " + pinotJson + "\n");
      _failedQueries.getAndIncrement();
      _compareStatusFileWriter.flush();
    }

    int totalQueries = _successfulQueries.get() + _failedQueries.get();

    if (displayStatus || totalQueries % 5000 == 0) {
      doDisplayStatus(totalQueries);
    }
  }

  private void doDisplayStatus(int nQueries) {
    long nowMs = System.currentTimeMillis();
    LOGGER.info("Failures {}/{} ({} empty query results)", _failedQueries.get(),
        (_failedQueries.get() + _successfulQueries.get()), _emptyResults);
    LOGGER.info("Query failure percentage {}% (empty result {}%, rate {} queries/s)",
        _failedQueries.get() * 100.0 / (_failedQueries.get() + _successfulQueries.get()),
        _emptyResults.get() * 100.00 / (_failedQueries.get() + _successfulQueries.get()),
        nQueries * 1000.0 / (nowMs - startTimeMs));
  }

  @Override
  protected void testQuery(@Nonnull String pqlQuery, @Nullable List<String> sqlQueries) throws Exception {
    runQuery(pqlQuery, _scanBasedQueryProcessor, false, null);
  }

  protected void runTestLoop(Callable<Object> testMethod) throws Exception {
    runTestLoop(testMethod, false);
  }

  protected void runTestLoop(Callable<Object> testMethod, boolean useMultipleThreads) throws Exception {
    // Clean up the Kafka topic
    // TODO jfim: Re-enable this once PINOT-2598 is fixed
    // purgeKafkaTopicAndResetRealtimeTable();

    List<Pair<File, File>> enabledRealtimeSegments = new ArrayList<>();

    // Sort the realtime segments based on their segment name so they get added from earliest to latest
    TreeMap<File, File> sortedRealtimeSegments = new TreeMap<File, File>(new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return _realtimeAvroToSegmentMap.get(o1).getName().compareTo(_realtimeAvroToSegmentMap.get(o2).getName());
      }
    });
    sortedRealtimeSegments.putAll(_realtimeAvroToSegmentMap);

    for (File avroFile: sortedRealtimeSegments.keySet()) {
      enabledRealtimeSegments.add(Pair.of(avroFile, sortedRealtimeSegments.get(avroFile)));

      if (useMultipleThreads) {
        _queryExecutor = new ThreadPoolExecutor(4, 4, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(50),
            new ThreadPoolExecutor.CallerRunsPolicy());
      }

      // Push avro for the new segment
      LOGGER.info("Pushing Avro file {} into Kafka", avroFile);
      ClusterIntegrationTestUtils.pushAvroIntoKafka(Collections.singletonList(avroFile),
          KafkaStarterUtils.DEFAULT_KAFKA_BROKER, _kafkaTopic, getMaxNumKafkaMessagesPerBatch(),
          getKafkaMessageHeader(), getPartitionColumn());

      // Configure the scan based comparator to use the distinct union of the offline and realtime segments
      configureScanBasedComparator(enabledRealtimeSegments);

      QueryResponse queryResponse = _scanBasedQueryProcessor.processQuery("select count(*) from mytable");

      _countStarResult = queryResponse.getNumDocsScanned();
      waitForAllDocsLoaded(getStabilizationTimeMs());

      // Run the actual tests
      LOGGER.info("Running queries");
      testMethod.call();

      if (useMultipleThreads) {
        if (_nQueriesRead == -1) {
          _queryExecutor.shutdown();
          _queryExecutor.awaitTermination(5, TimeUnit.MINUTES);
        } else {
          int totalQueries = _failedQueries.get() + _successfulQueries.get();
          while (totalQueries < _nQueriesRead) {
            LOGGER.info("Completed " + totalQueries + " out of " + _nQueriesRead + " - waiting");
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
            totalQueries = _failedQueries.get() + _successfulQueries.get();
          }
          if (totalQueries > _nQueriesRead) {
            throw new RuntimeException("Executed " + totalQueries + " more than " + _nQueriesRead);
          }
          _queryExecutor.shutdown();
        }
      }
      int totalQueries = _failedQueries.get() + _successfulQueries.get();
      doDisplayStatus(totalQueries);

      // Release resources
      _scanBasedQueryProcessor.close();
      _compareStatusFileWriter.write("Status after push of " + avroFile + ":" + System.currentTimeMillis() +
          ":Executed " + _nQueriesRead + " queries, " + _failedQueries + " failures," + _emptyResults.get()
          + " empty results\n");
    }
  }

  private void configureScanBasedComparator(List<Pair<File, File>> enabledRealtimeSegments) throws Exception {
    // Deduplicate overlapping realtime and offline segments
    Set<String> enabledAvroFiles = new HashSet<String>();
    List<File> segmentsToUnpack = new ArrayList<File>();

    LOGGER.info("Offline avro to segment map {}", _offlineAvroToSegmentMap);
    LOGGER.info("Enabled realtime segments {}", enabledRealtimeSegments);

    for (Map.Entry<File, File> avroAndSegment: _offlineAvroToSegmentMap.entrySet()) {
      if (!enabledAvroFiles.contains(avroAndSegment.getKey().getName())) {
        enabledAvroFiles.add(avroAndSegment.getKey().getName());
        segmentsToUnpack.add(avroAndSegment.getValue());
      }
    }

    for (Pair<File, File> avroAndSegment : enabledRealtimeSegments) {
      if (!enabledAvroFiles.contains(avroAndSegment.getLeft().getName())) {
        enabledAvroFiles.add(avroAndSegment.getLeft().getName());
        segmentsToUnpack.add(avroAndSegment.getRight());
      }
    }

    LOGGER.info("Enabled Avro files {}", enabledAvroFiles);

    // Unpack enabled segments
    TestUtils.ensureDirectoriesExistAndEmpty(_unpackedSegments);
    for (File file : segmentsToUnpack) {
      LOGGER.info("Unpacking file {}", file);
      TarGzCompressionUtils.unTar(file, _unpackedSegments);
    }

    _scanBasedQueryProcessor = new ScanBasedQueryProcessor(_unpackedSegments.getAbsolutePath());
  }

  protected int getNumSuccesfulQueries() {
    return _successfulQueries.get();
  }

  protected int getNumFailedQueries() {
    return _failedQueries.get();
  }

  protected int getNumEmptyResults() {
    return _emptyResults.get();
  }

  private void purgeKafkaTopicAndResetRealtimeTable() throws Exception {
    // Drop the realtime table
    dropRealtimeTable("mytable");
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

    // Drop and recreate the Kafka topic
    KafkaStarterUtils.deleteTopic(_kafkaTopic, KafkaStarterUtils.DEFAULT_ZK_STR);
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    KafkaStarterUtils.createTopic(_kafkaTopic, KafkaStarterUtils.DEFAULT_ZK_STR, 10);

    // Recreate the realtime table
    addRealtimeTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", 900, "Days", KafkaStarterUtils.DEFAULT_ZK_STR,
        _kafkaTopic, _schema.getSchemaName(), "TestTenant", "TestTenant",
        _realtimeAvroToSegmentMap.keySet().iterator().next(), 1000000, getSortedColumn(), new ArrayList<String>(), null,
        null, null);
  }

  @Override
  @Test(enabled = false)
  public void testHardcodedQueries()
      throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call()
          throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testHardcodedQueries();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testQueriesFromQueryFile()
      throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call()
          throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testQueriesFromQueryFile();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithoutMultiValues()
      throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call()
          throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testGeneratedQueriesWithoutMultiValues();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call()
          throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testGeneratedQueriesWithMultiValues();
        return null;
      }
    });
  }

  @Test(enabled = false)
  @Override
  public void testInstanceShutdown()
      throws Exception {
    // Skipped
  }

  @Override
  @Test(enabled = false)
  public void testBrokerDebugOutput()
      throws Exception {
    // Skipped
  }

  @Override
  protected int getRealtimeSegmentFlushSize(boolean useLlc) {
    return super.getRealtimeSegmentFlushSize(useLlc) * 10;
  }
}
