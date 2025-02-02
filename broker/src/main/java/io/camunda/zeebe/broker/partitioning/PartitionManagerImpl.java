/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.partitioning;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.primitive.partition.impl.DefaultPartitionManagementService;
import io.atomix.raft.partition.RaftPartition;
import io.camunda.zeebe.broker.PartitionListener;
import io.camunda.zeebe.broker.PartitionRaftListener;
import io.camunda.zeebe.broker.clustering.ClusterServices;
import io.camunda.zeebe.broker.exporter.repo.ExporterRepository;
import io.camunda.zeebe.broker.partitioning.startup.PartitionStartupContext;
import io.camunda.zeebe.broker.partitioning.startup.RaftPartitionFactory;
import io.camunda.zeebe.broker.partitioning.startup.ZeebePartitionFactory;
import io.camunda.zeebe.broker.partitioning.topology.PartitionDistribution;
import io.camunda.zeebe.broker.partitioning.topology.TopologyManager;
import io.camunda.zeebe.broker.partitioning.topology.TopologyManagerImpl;
import io.camunda.zeebe.broker.partitioning.topology.TopologyPartitionListener;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.monitoring.BrokerHealthCheckService;
import io.camunda.zeebe.broker.system.monitoring.DiskSpaceUsageMonitor;
import io.camunda.zeebe.broker.system.partitions.PartitionHealthBroadcaster;
import io.camunda.zeebe.broker.system.partitions.ZeebePartition;
import io.camunda.zeebe.broker.transport.commandapi.CommandApiService;
import io.camunda.zeebe.engine.processing.streamprocessor.JobStreamer;
import io.camunda.zeebe.protocol.impl.encoding.BrokerInfo;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.ConcurrencyControl;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.ActorFutureCollector;
import io.camunda.zeebe.topology.changes.PartitionChangeExecutor;
import io.camunda.zeebe.transport.impl.AtomixServerTransport;
import io.camunda.zeebe.util.health.HealthStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PartitionManagerImpl
    implements PartitionManager, TopologyManager, PartitionChangeExecutor {

  public static final String GROUP_NAME = "raft-partition";

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManagerImpl.class);
  private final ConcurrencyControl concurrencyControl;

  private final BrokerHealthCheckService healthCheckService;
  private final ActorSchedulingService actorSchedulingService;
  private final TopologyManagerImpl topologyManager;

  private final List<ZeebePartition> zeebePartitions = new CopyOnWriteArrayList<>();
  private final List<RaftPartition> raftPartitions = new CopyOnWriteArrayList<>();
  private final Map<Integer, PartitionAdminAccess> adminAccess = new ConcurrentHashMap<>();
  private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
  private final DiskSpaceUsageMonitor diskSpaceUsageMonitor;
  private final PartitionDistribution partitionDistribution;
  private final DefaultPartitionManagementService managementService;
  private final BrokerCfg brokerCfg;
  private final ZeebePartitionFactory zeebePartitionFactory;
  private final RaftPartitionFactory raftPartitionFactory;

  public PartitionManagerImpl(
      final ConcurrencyControl concurrencyControl,
      final ActorSchedulingService actorSchedulingService,
      final BrokerCfg brokerCfg,
      final BrokerInfo localBroker,
      final ClusterServices clusterServices,
      final BrokerHealthCheckService healthCheckService,
      final DiskSpaceUsageMonitor diskSpaceUsageMonitor,
      final List<PartitionListener> partitionListeners,
      final List<PartitionRaftListener> partitionRaftListeners,
      final CommandApiService commandApiService,
      final ExporterRepository exporterRepository,
      final AtomixServerTransport gatewayBrokerTransport,
      final JobStreamer jobStreamer,
      final PartitionDistribution partitionDistribution) {
    this.brokerCfg = brokerCfg;
    this.concurrencyControl = concurrencyControl;
    this.actorSchedulingService = actorSchedulingService;
    this.healthCheckService = healthCheckService;
    this.diskSpaceUsageMonitor = diskSpaceUsageMonitor;
    final var featureFlags = brokerCfg.getExperimental().getFeatures().toFeatureFlags();
    this.partitionDistribution = partitionDistribution;
    // TODO: Do this as a separate step before starting the partition manager
    topologyManager = new TopologyManagerImpl(clusterServices.getMembershipService(), localBroker);

    final List<PartitionListener> listeners = new ArrayList<>(partitionListeners);
    listeners.add(topologyManager);

    zeebePartitionFactory =
        new ZeebePartitionFactory(
            actorSchedulingService,
            brokerCfg,
            localBroker,
            commandApiService,
            clusterServices,
            exporterRepository,
            healthCheckService,
            diskSpaceUsageMonitor,
            gatewayBrokerTransport,
            jobStreamer,
            listeners,
            partitionRaftListeners,
            topologyManager,
            featureFlags);
    managementService =
        new DefaultPartitionManagementService(
            clusterServices.getMembershipService(), clusterServices.getCommunicationService());
    raftPartitionFactory = new RaftPartitionFactory(brokerCfg);
  }

  public PartitionAdminAccess createAdminAccess(final ConcurrencyControl concurrencyControl) {
    return new MultiPartitionAdminAccess(concurrencyControl, adminAccess);
  }

  public void start() {
    actorSchedulingService.submitActor(topologyManager);
    final var localMemberId = managementService.getMembershipService().getLocalMember().id();
    final var memberPartitions =
        partitionDistribution.partitions().stream()
            .filter(p -> p.members().contains(localMemberId))
            .toList();

    healthCheckService.registerBootstrapPartitions(memberPartitions);
    for (final var partitionMetadata : memberPartitions) {
      bootstrapPartition(partitionMetadata);
    }
  }

  private ActorFuture<Void> bootstrapPartition(final PartitionMetadata partitionMetadata) {
    final var result = concurrencyControl.<Void>createFuture();
    final var id = partitionMetadata.id().id();
    final var context =
        new PartitionStartupContext(
            actorSchedulingService,
            concurrencyControl,
            managementService,
            partitionMetadata,
            raftPartitionFactory,
            zeebePartitionFactory,
            brokerCfg);
    final var partition = Partition.bootstrapping(context);
    partitions.put(id, partition);

    concurrencyControl.runOnCompletion(
        partition.start(), (started, error) -> completePartitionStart(id, started, error, result));
    return result;
  }

  private ActorFuture<Void> joinPartition(final PartitionMetadata partitionMetadata) {
    final var result = concurrencyControl.<Void>createFuture();
    final var id = partitionMetadata.id().id();
    final var context =
        new PartitionStartupContext(
            actorSchedulingService,
            concurrencyControl,
            managementService,
            partitionMetadata,
            raftPartitionFactory,
            zeebePartitionFactory,
            brokerCfg);
    final var partition = Partition.joining(context);
    final var previousPartition = partitions.putIfAbsent(id, partition);
    if (previousPartition != null) {
      result.completeExceptionally(
          new IllegalStateException(String.format("Partition %d already exists", id)));
      return result;
    }
    concurrencyControl.run(
        () ->
            concurrencyControl.runOnCompletion(
                partition.start(),
                (started, error) -> completePartitionStart(id, started, error, result)));
    return result;
  }

  private void completePartitionStart(
      final int partitionId,
      final Partition partition,
      final Throwable error,
      final ActorFuture<Void> future) {

    if (error != null) {
      LOGGER.error("Failed to start partition {}", partitionId, error);
      onHealthChanged(partitionId, HealthStatus.DEAD);
      future.completeExceptionally(error);
      return;
    }

    LOGGER.info("Started partition {}", partitionId);
    final var zeebePartition = partition.zeebePartition();
    final var raftPartition = partition.raftPartition();

    zeebePartition.addFailureListener(
        new PartitionHealthBroadcaster(partitionId, this::onHealthChanged));
    diskSpaceUsageMonitor.addDiskUsageListener(zeebePartition);
    adminAccess.put(partitionId, zeebePartition.getAdminAccess());
    zeebePartitions.add(zeebePartition);
    raftPartitions.add(raftPartition);
    future.complete(null);
  }

  public ActorFuture<Void> stop() {
    final var result = concurrencyControl.<Void>createFuture();
    final var stop =
        partitions.values().stream()
            .map(Partition::stop)
            .collect(new ActorFutureCollector<>(concurrencyControl));
    concurrencyControl.runOnCompletion(
        stop,
        (ok, error) -> {
          if (error != null) {
            LOGGER.error("Failed to stop partitions", error);
            result.completeExceptionally(error);
          } else {
            partitions.clear();
            raftPartitions.clear();
            zeebePartitions.clear();
            adminAccess.clear();
            topologyManager.closeAsync().onComplete(result);
          }
        });
    return result;
  }

  @Override
  public String toString() {
    return "PartitionManagerImpl{partitions=" + zeebePartitions + '}';
  }

  public void onHealthChanged(final int i, final HealthStatus healthStatus) {
    topologyManager.onHealthChanged(i, healthStatus);
  }

  @Override
  public void removeTopologyPartitionListener(final TopologyPartitionListener listener) {
    topologyManager.removeTopologyPartitionListener(listener);
  }

  @Override
  public void addTopologyPartitionListener(final TopologyPartitionListener listener) {
    topologyManager.addTopologyPartitionListener(listener);
  }

  @Override
  public RaftPartition getRaftPartition(final int partitionId) {
    return raftPartitions.stream().filter(p -> p.id().id() == partitionId).findFirst().orElse(null);
  }

  @Override
  public Collection<RaftPartition> getRaftPartitions() {
    return raftPartitions;
  }

  @Override
  public Collection<ZeebePartition> getZeebePartitions() {
    return zeebePartitions;
  }

  @Override
  public ActorFuture<Void> join(
      final int partitionId, final Map<MemberId, Integer> membersWithPriority) {
    final int targetPriority = Collections.max(membersWithPriority.values());

    final var members = membersWithPriority.keySet();
    final var primaries =
        membersWithPriority.entrySet().stream()
            .filter(entry -> entry.getValue() == targetPriority)
            .map(Entry::getKey)
            .toList();

    MemberId primary = null;
    if (primaries.size() == 1) {
      primary = primaries.get(0);
    }

    final var partitionMetadata =
        new PartitionMetadata(
            PartitionId.from(GROUP_NAME, partitionId),
            members,
            membersWithPriority,
            targetPriority,
            primary);

    return joinPartition(partitionMetadata);
  }

  @Override
  public ActorFuture<Void> leave(final int partitionId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
