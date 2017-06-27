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

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import java.io.File;
import java.util.List;
import org.apache.avro.reflect.Nullable;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 */
public class LLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final int NUM_KAFKA_BROKERS = 2;
  private static final int NUM_KAFKA_PARTITIONS = 2;

  @Override
  protected int getNumKafkaBrokers() {
    return NUM_KAFKA_BROKERS;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_KAFKA_PARTITIONS;
  }

  @Nullable
  @Override
  protected String getLoadMode() {
    return "MMAP";
  }

  @Override
  protected void setUpTable(String timeColumnName, String timeColumnType, File avroFile)
      throws Exception {
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    String schemaName = schema.getSchemaName();
    addSchema(schemaFile, schemaName);
    addLLCRealtimeTable(getTableName(), timeColumnName, timeColumnType, -1, null,
        KafkaStarterUtils.DEFAULT_KAFKA_BROKER, getKafkaTopic(), schemaName, null, null, avroFile,
        getRealtimeSegmentFlushSize(true), getSortedColumn(), getInvertedIndexColumns(), getLoadMode(),
        getRawIndexColumns(), getTaskConfig());
  }

  @Test
  public void testSegmentFlushSize()
      throws Exception {
    String zkSegmentsPath = "/SEGMENTS/" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<String> segmentNames = _propertyStore.getChildNames(zkSegmentsPath, 0);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = _propertyStore.get(zkSegmentsPath + "/" + segmentName, null, 0);
      Assert.assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(getRealtimeSegmentFlushSize(true) / getNumKafkaPartitions()), "Segment: " + segmentName +
              " does not have the expected flush size");
    }
  }
}
