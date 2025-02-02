/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.partitioning.startup;

import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.raft.partition.RaftPartition;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.partitions.ZeebePartition;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.ConcurrencyControl;
import io.camunda.zeebe.snapshots.impl.FileBasedSnapshotStore;
import java.nio.file.Path;

public final class PartitionStartupContext {
  private final ActorSchedulingService schedulingService;
  private final ConcurrencyControl concurrencyControl;
  private final PartitionManagementService partitionManagementService;
  private final PartitionMetadata partitionMetadata;
  private final RaftPartitionFactory raftPartitionFactory;
  private final ZeebePartitionFactory zeebePartitionFactory;
  private final BrokerCfg brokerConfig;

  private Path partitionDirectory;

  private FileBasedSnapshotStore snapshotStore;
  private RaftPartition raftPartition;
  private ZeebePartition zeebePartition;

  public PartitionStartupContext(
      final ActorSchedulingService schedulingService,
      final ConcurrencyControl concurrencyControl,
      final PartitionManagementService partitionManagementService,
      final PartitionMetadata partitionMetadata,
      final RaftPartitionFactory raftPartitionFactory,
      final ZeebePartitionFactory zeebePartitionFactory,
      final BrokerCfg brokerConfig) {
    this.schedulingService = schedulingService;
    this.concurrencyControl = concurrencyControl;
    this.partitionManagementService = partitionManagementService;
    this.partitionMetadata = partitionMetadata;
    this.raftPartitionFactory = raftPartitionFactory;
    this.zeebePartitionFactory = zeebePartitionFactory;
    this.brokerConfig = brokerConfig;
  }

  @Override
  public String toString() {
    return "PartitionStartupContext{" + "partition=" + partitionMetadata.id().id() + '}';
  }

  public ActorSchedulingService schedulingService() {
    return schedulingService;
  }

  public ConcurrencyControl concurrencyControl() {
    return concurrencyControl;
  }

  public PartitionManagementService partitionManagementService() {
    return partitionManagementService;
  }

  public PartitionMetadata partitionMetadata() {
    return partitionMetadata;
  }

  public RaftPartitionFactory raftPartitionFactory() {
    return raftPartitionFactory;
  }

  public ZeebePartitionFactory zeebePartitionFactory() {
    return zeebePartitionFactory;
  }

  public Path partitionDirectory() {
    return partitionDirectory;
  }

  public FileBasedSnapshotStore snapshotStore() {
    return snapshotStore;
  }

  public PartitionStartupContext snapshotStore(final FileBasedSnapshotStore snapshotStore) {
    this.snapshotStore = snapshotStore;
    return this;
  }

  public PartitionStartupContext raftPartition(final RaftPartition raftPartition) {
    this.raftPartition = raftPartition;
    return this;
  }

  public RaftPartition raftPartition() {
    return raftPartition;
  }

  public PartitionStartupContext zeebePartition(final ZeebePartition zeebePartition) {
    this.zeebePartition = zeebePartition;
    return this;
  }

  public ZeebePartition zeebePartition() {
    return zeebePartition;
  }

  public BrokerCfg brokerConfig() {
    return brokerConfig;
  }

  public PartitionStartupContext partitionDirectory(final Path partitionDirectory) {
    this.partitionDirectory = partitionDirectory;
    return this;
  }
}
