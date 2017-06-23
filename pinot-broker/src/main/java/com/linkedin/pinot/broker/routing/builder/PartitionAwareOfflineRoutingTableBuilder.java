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

import com.linkedin.pinot.broker.pruner.SegmentZKMetadataPrunerService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;

public class PartitionAwareOfflineRoutingTableBuilder extends AbstractPartitionAwareRoutingTableBuilder {
  private static final int TABLE_LEVEL_PARTITION_NUMBER = 0;

  private boolean _isPartitionLevelReplicaGroupAssignment;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableConfig = tableConfig;
    _propertyStore = propertyStore;
    _pruner = new SegmentZKMetadataPrunerService(new String[]{PARTITION_METADATA_PRUNER});

    final String partitionColumn = _tableConfig.getValidationConfig().getReplicaGroupStrategyConfig().getPartitionColumn();
    _isPartitionLevelReplicaGroupAssignment = (partitionColumn != null);
  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigList) {
    RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);
    String[] segmentSet = externalView.getPartitionSet().toArray(new String[externalView.getPartitionSet().size()]);
    _numReplicas = _tableConfig.getValidationConfig().getReplicationNumber();
    PartitionToReplicaGroupMappingZKMetadata partitionToReplicaGroupMappingZKMedata = ZKMetadataProvider.getPartitionToReplicaGroupMappingZKMedata(_propertyStore, tableName);

    // Compute partition id set
    Set<Integer> partitionIds = new HashSet<>();
    for (String segment : segmentSet) {
      SegmentId segmentId = new SegmentId(segment);
      // retrieve the metadata for the segment and compute the partitionIds set
      SegmentZKMetadata segmentZKMetadata = _segment2SegmentZkMetadataMap.get(segmentId);
      if (segmentZKMetadata == null || segmentZKMetadata.getPartitionMetadata() == null) {
        segmentZKMetadata = ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segment);
        _segment2SegmentZkMetadataMap.put(segmentId, segmentZKMetadata);
      }
      int partitionId = getPartitionId(segmentZKMetadata);
      if (partitionId != NO_PARTITION_NUMBER) {
        partitionIds.add(partitionId);
      }
    }

    // Compute partition to server to replica id mapping for each partition
    Map<Integer, Map<ServerInstance, Integer>> perPartitionServer2ReplicaIdMapping = new HashMap<>();
    for (Integer partitionId : partitionIds) {
      for (int replicaId = 0; replicaId < _numReplicas; replicaId++) {
        List<String> instancesfromReplicaGroup = partitionToReplicaGroupMappingZKMedata.getInstancesfromReplicaGroup(partitionId, replicaId);
        for (String instanceName : instancesfromReplicaGroup) {
          if(!perPartitionServer2ReplicaIdMapping.containsKey(partitionId)) {
            perPartitionServer2ReplicaIdMapping.put(partitionId, new HashMap<ServerInstance, Integer>());
          }
          perPartitionServer2ReplicaIdMapping.get(partitionId).put(ServerInstance.forInstanceName(instanceName), replicaId);
        }
      }
    }

    // Compute segment id to replica id to server instance
    Map<SegmentId, Map<Integer, ServerInstance>> segmentId2ServersMapping = new HashMap<>();
    for (String segment : segmentSet) {
      SegmentId segmentId = new SegmentId(segment);
      SegmentZKMetadata segmentZKMetadata = _segment2SegmentZkMetadataMap.get(segmentId);
      int partitionId = getPartitionId(segmentZKMetadata);
      Map<String, String> instanceToStateMap = new HashMap<>(externalView.getStateMap(segment));
      Map<Integer, ServerInstance> serverInstanceMap = new HashMap<>();
      int replicaIdForNoPartitionMetadata = 0;
      for (String instance : instanceToStateMap.keySet()) {
        if (pruner.isInactive(instance)) {
          continue;
        }
        if (instanceToStateMap.get(instance).equals("ONLINE")) {
          // If there's no partition number in the metadata, assign replica id to instance sequentially.
          ServerInstance serverInstance = ServerInstance.forInstanceName(instance);
          if (partitionId == NO_PARTITION_NUMBER) {
            serverInstanceMap.put(replicaIdForNoPartitionMetadata, serverInstance);
            replicaIdForNoPartitionMetadata++;
          } else {
            int replicaId = perPartitionServer2ReplicaIdMapping.get(partitionId).get(serverInstance);
            serverInstanceMap.put(replicaId, serverInstance);
          }
        }
      }
      segmentId2ServersMapping.put(segmentId, serverInstanceMap);
    }

    // Delete segment metadata from cache if the segment no longer exists in the external view.
    Set<String> segmentsFromExternalView = new HashSet<>(Arrays.asList(segmentSet));
    for (SegmentId segmentId : _segment2SegmentZkMetadataMap.keySet()) {
      if (!segmentsFromExternalView.contains(segmentId.getSegmentId())) {
        _segment2SegmentZkMetadataMap.remove(segmentId);
      }
    }

    _mappingRef.set(segmentId2ServersMapping);
  }

  /**
   * Assumes there is only one column
   *
   * @param segmentZKMetadata
   * @return
   */
  private int getPartitionId(SegmentZKMetadata segmentZKMetadata) {
    // If we use the partition level replica group assignment, we need to get the partition id by looking at the
    // segment metadata.
    if (_isPartitionLevelReplicaGroupAssignment) {
      SegmentPartitionMetadata partitionMetadata = segmentZKMetadata.getPartitionMetadata();
      if (partitionMetadata == null) {
        return NO_PARTITION_NUMBER;
      }
      Map<String, ColumnPartitionMetadata> columnPartitionMap = partitionMetadata.getColumnPartitionMap();
      if (columnPartitionMap == null || columnPartitionMap.size() == 0) {
        return NO_PARTITION_NUMBER;
      }
      ColumnPartitionMetadata columnPartitionMetadata;
      if (columnPartitionMap.size() == 1) {
        columnPartitionMetadata = columnPartitionMap.values().iterator().next();
        int partitionIdStart = columnPartitionMetadata.getPartitionRanges().get(0).getMaximumInteger();
        // int partitionIdEnd = columnPartitionMetadata.getPartitionRanges().get(0).getMaximumInteger();
        return partitionIdStart;
      }
    }
    // If we use the table level replica group assignment, we can simply return the default partition number.
    return TABLE_LEVEL_PARTITION_NUMBER;
  }
}
