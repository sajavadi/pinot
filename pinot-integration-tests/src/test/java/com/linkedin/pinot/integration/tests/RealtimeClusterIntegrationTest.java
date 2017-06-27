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

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and queries Pinot.
 */
public class RealtimeClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private List<KafkaServerStartable> _kafkaStarters;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    ExecutorService executor = Executors.newCachedThreadPool();

    // Push data into the Kafka topic
    pushAvroIntoKafka(avroFiles, getKafkaTopic(), executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create Pinot table
    setUpTable("DaysSinceEpoch", "DAYS", avroFiles.get(0));

    // Wait for all documents loaded
    waitForAllDocsLoaded(60_000L);
  }

  protected void startKafka() {
    _kafkaStarters = KafkaStarterUtils.startServers(getNumKafkaBrokers(), KafkaStarterUtils.DEFAULT_KAFKA_PORT,
        KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());
    KafkaStarterUtils.createTopic(getKafkaTopic(), KafkaStarterUtils.DEFAULT_ZK_STR, getNumKafkaPartitions());
  }

  protected void setUpTable(String timeColumnName, String timeColumnType, File avroFile)
      throws Exception {
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    String schemaName = schema.getSchemaName();
    addSchema(schemaFile, schemaName);
    addRealtimeTable(getTableName(), timeColumnName, timeColumnType, -1, null, KafkaStarterUtils.DEFAULT_ZK_STR,
        getKafkaTopic(), schemaName, null, null, avroFile, getRealtimeSegmentFlushSize(false), getSortedColumn(),
        getInvertedIndexColumns(), getLoadMode(), getRawIndexColumns(), getTaskConfig());
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

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    for (KafkaServerStartable kafkaStarter : _kafkaStarters) {
      KafkaStarterUtils.stopServer(kafkaStarter);
    }
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
