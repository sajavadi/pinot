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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.broker.pruner.SegmentPrunerContext;
import com.linkedin.pinot.broker.pruner.SegmentZKMetadataPrunerService;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public abstract class AbstractPartitionAwareRoutingTableBuilder extends AbstractRoutingTableBuilder {
  protected static final String PARTITION_METADATA_PRUNER = "PartitionZKMetadataPruner";
  protected static final int NO_PARTITION_NUMBER = -1;

  Map<SegmentId, Map<Integer, ServerInstance>> _segmentId2ServersMapping = new HashMap<>();
  AtomicReference<Map<SegmentId, Map<Integer, ServerInstance>>> _mappingRef = new AtomicReference<>(_segmentId2ServersMapping);
  Map<SegmentId, SegmentZKMetadata> _segment2SegmentZkMetadataMap = new ConcurrentHashMap<>();
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected SegmentZKMetadataPrunerService _pruner;
  protected TableConfig _tableConfig;
  protected Random _random = new Random();
  protected int _numReplicas;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableConfig = tableConfig;
    _propertyStore = propertyStore;
    _pruner = new SegmentZKMetadataPrunerService(new String[]{PARTITION_METADATA_PRUNER});
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    Map<ServerInstance, SegmentIdSet> result = new HashMap<>();
    Map<SegmentId, Map<Integer, ServerInstance>> mappingReference = _mappingRef.get();
    SegmentPrunerContext prunerContext = new SegmentPrunerContext(request.getBrokerRequest());
    int replicaGroupId = _random.nextInt(_numReplicas);
    for (SegmentId segmentId : mappingReference.keySet()) {
      // Check if the segment can be pruned
      SegmentZKMetadata segmentZKMetadata = _segment2SegmentZkMetadataMap.get(segmentId);
      boolean segmentPruned = _pruner.prune(segmentZKMetadata, prunerContext);

      // If the segment cannot be pruned, we need to pick the server.
      if (!segmentPruned) {
        Map<Integer, ServerInstance> replicaId2ServerMapping = mappingReference.get(segmentId);
        ServerInstance serverInstance = replicaId2ServerMapping.get(replicaGroupId);
        // pick any other available server instance when the node is down/disabled
        if (serverInstance == null) {
          if (!replicaId2ServerMapping.isEmpty()) {
            serverInstance = replicaId2ServerMapping.values().iterator().next();
          } else {
            // No server is found for this segment.
            continue;
          }
        }
        SegmentIdSet segmentIdSet = result.get(serverInstance);
        if (segmentIdSet == null) {
          segmentIdSet = new SegmentIdSet();
          result.put(serverInstance, segmentIdSet);
        }
        segmentIdSet.addSegment(segmentId);
      }
    }
    return result;
  }

  @Override
  public boolean isPartitionAware() {
    return true;
  }
}
