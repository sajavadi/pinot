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

import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;


/**
 * Partition aware routing table builder for the Kafka low level consumer
 */
public class PartitionAwareRealtimeRoutingTableBuilder extends AbstractPartitionAwareRoutingTableBuilder {

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {

    _numReplicas = Integer.valueOf(_tableConfig.getValidationConfig().getReplicasPerPartition());

    String path = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(tableName);
    ZNRecord kafkaPartitionAssignment = _propertyStore.get(path, null, AccessOption.PERSISTENT);

    // Compute partition id set
    Set<Integer> partitionIds = new HashSet<>();
    for (String segment : externalView.getPartitionSet()) {
      SegmentId segmentId = new SegmentId(segment);

      // retrieve the metadata for the segment and compute the partitionIds set
      SegmentZKMetadata segmentZKMetadata = _segment2SegmentZkMetadataMap.get(segmentId);
      if (segmentZKMetadata == null || segmentZKMetadata.getPartitionMetadata() == null) {
        segmentZKMetadata = ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, tableName, segment);
        _segment2SegmentZkMetadataMap.put(segmentId, segmentZKMetadata);
      }
      partitionIds.add(getPartitionId(segment));
    }

    // Compute partition to server to replica id mapping for each partition from Kafka partition mapping
    Map<Integer, Map<ServerInstance, Integer>> serverToReplicaIdByPartitionId = new HashMap<>();
    for (Integer partitionId : partitionIds) {
      String partitionIdString = String.valueOf(partitionId);
      List<String> instancesFromPartition = kafkaPartitionAssignment.getListField(partitionIdString);
      for (int replicaId = 0; replicaId < instancesFromPartition.size(); replicaId++) {
        String instanceName = instancesFromPartition.get(replicaId);
        if (!serverToReplicaIdByPartitionId.containsKey(partitionId)) {
          serverToReplicaIdByPartitionId.put(partitionId, new HashMap<ServerInstance, Integer>());
        }
        serverToReplicaIdByPartitionId.get(partitionId)
            .put(ServerInstance.forInstanceName(instanceName), replicaId);
      }
    }

    // TODO: This part of code is borrowed from `KafkaLowlevelConsumerRoutingTableBuilder`. This needs to be cleaned.

    // Start of the copy code

    // 1. Gather all segments and group them by Kafka partition, sorted by sequence number
    Map<String, SortedSet<SegmentName>> sortedSegmentsByKafkaPartition = new HashMap<String, SortedSet<SegmentName>>();
    for (String helixPartitionName : externalView.getPartitionSet()) {
      // Ignore segments that are not low level consumer segments
      if (!SegmentName.isLowLevelConsumerSegmentName(helixPartitionName)) {
        continue;
      }

      final LLCSegmentName segmentName = new LLCSegmentName(helixPartitionName);
      String kafkaPartitionName = segmentName.getPartitionRange();
      SortedSet<SegmentName> segmentsForPartition = sortedSegmentsByKafkaPartition.get(kafkaPartitionName);

      // Create sorted set if necessary
      if (segmentsForPartition == null) {
        segmentsForPartition = new TreeSet<>();

        sortedSegmentsByKafkaPartition.put(kafkaPartitionName, segmentsForPartition);
      }

      segmentsForPartition.add(segmentName);
    }

    // 2. Ensure that for each Kafka partition, we have at most one Helix partition (Pinot segment) in consuming state
    Map<String, SegmentName> allowedSegmentInConsumingStateByKafkaPartition = new HashMap<>();
    for (String kafkaPartition : sortedSegmentsByKafkaPartition.keySet()) {
      SortedSet<SegmentName> sortedSegmentsForKafkaPartition = sortedSegmentsByKafkaPartition.get(kafkaPartition);
      SegmentName lastAllowedSegmentInConsumingState = null;

      for (SegmentName segmentName : sortedSegmentsForKafkaPartition) {
        Map<String, String> helixPartitionState = externalView.getStateMap(segmentName.getSegmentName());
        boolean allInConsumingState = true;
        int replicasInConsumingState = 0;

        // Only keep the segment if all replicas have it in CONSUMING state
        for (String externalViewState : helixPartitionState.values()) {
          // Ignore ERROR state
          if (externalViewState.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ERROR)) {
            continue;
          }

          // Not all segments are in CONSUMING state, therefore don't consider the last segment assignable to CONSUMING
          // replicas
          if (externalViewState.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
            allInConsumingState = false;
            break;
          }

          // Otherwise count the replica as being in CONSUMING state
          if (externalViewState.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
            replicasInConsumingState++;
          }
        }

        // If all replicas have this segment in consuming state (and not all of them are in ERROR state), then pick this
        // segment to be the last allowed segment to be in CONSUMING state
        if (allInConsumingState && 0 < replicasInConsumingState) {
          lastAllowedSegmentInConsumingState = segmentName;
          break;
        }
      }

      if (lastAllowedSegmentInConsumingState != null) {
        allowedSegmentInConsumingStateByKafkaPartition.put(kafkaPartition, lastAllowedSegmentInConsumingState);
      }
    }
    // End of the copy code.

    RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);

    // Compute segment id to replica id to server instance
    Map<SegmentId, Map<Integer, ServerInstance>> segmentId2ServersMapping = new HashMap<>();
    for (String segmentName : externalView.getPartitionSet()) {
      SegmentId segmentId = new SegmentId(segmentName);
      int partitionId = getPartitionId(segmentName);
      Map<String, String> instanceToStateMap = new HashMap<>(externalView.getStateMap(segmentName));
      Map<Integer, ServerInstance> serverInstanceMap = new HashMap<>();

      String partitionStr = Integer.toString(partitionId);
      SegmentName validConsumingSegment = allowedSegmentInConsumingStateByKafkaPartition.get(partitionStr);
      for (String instance : instanceToStateMap.keySet()) {
        if (pruner.isInactive(instance)) {
          continue;
        }

        String state = instanceToStateMap.get(instance);
        ServerInstance serverInstance = ServerInstance.forInstanceName(instance);
        int replicaId = serverToReplicaIdByPartitionId.get(partitionId).get(serverInstance);

        // If the server is in ONLINE status, it's always to safe to add
        if (state.equalsIgnoreCase(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
          serverInstanceMap.put(replicaId, serverInstance);
        }

        // If the server is in CONSUMING status, the segment has to be match with the valid consuming segment
        if (state.equalsIgnoreCase(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          if (validConsumingSegment != null && segmentName.equals(validConsumingSegment.getSegmentName())) {
            serverInstanceMap.put(replicaId, serverInstance);
          }
        }
      }
      segmentId2ServersMapping.put(segmentId, serverInstanceMap);
    }

    _mappingRef.set(segmentId2ServersMapping);
  }

  /**
   * Retreive the partition Id from the segment name of the realtime segment
   *
   * @param segmentName
   * @return
   */
  private int getPartitionId(String segmentName) {
    final LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String kafkaPartitionName = llcSegmentName.getPartitionRange();
    return Integer.valueOf(kafkaPartitionName);
  }
}
