/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.topology;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.broker.partitioning.topology.StaticPartitionDistributionResolver;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.scheduler.ConcurrencyControl;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responsible for initializing ClusterTopology and managing ClusterTopology changes. */
final class ClusterTopologyManager {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterTopologyManager.class);

  private final ConcurrencyControl executor;
  private final PersistedClusterTopology persistedClusterTopology;
  private final ClusterTopologyGossipHandler gossipHandler;

  ClusterTopologyManager(
      final ConcurrencyControl executor,
      final PersistedClusterTopology persistedClusterTopology,
      final ClusterTopologyGossipHandler gossipHandler) {
    this.executor = executor;
    this.persistedClusterTopology = persistedClusterTopology;
    this.gossipHandler = gossipHandler;
  }

  ActorFuture<ClusterTopology> getClusterTopology() {
    return executor.call(persistedClusterTopology::getTopology);
  }

  ActorFuture<Void> start(final BrokerCfg brokerCfg) {
    final ActorFuture<Void> startFuture = executor.createFuture();

    executor.run(
        () -> {
          try {
            gossipHandler.registerListener(
                otherTopology ->
                    executor.run(() -> handleClusterTopologyFromAnotherMember(otherTopology)));

            initialize(brokerCfg);
            startFuture.complete(null);
          } catch (final Exception e) {
            LOG.error("Failed to initialize ClusterTopology", e);
            startFuture.completeExceptionally(e);
          }
        });

    return startFuture;
  }

  private void handleClusterTopologyFromAnotherMember(final ClusterTopology otherClusterTopology) {
    final var currentTopology = persistedClusterTopology.getTopology();
    if (otherClusterTopology.equals(currentTopology)) {
      return;
    }

    final var newTopology = otherClusterTopology.merge(currentTopology);
    if (newTopology.equals(currentTopology)) {
      return;
    }

    try {
      updateLocalTopology(newTopology);
    } catch (final IOException e) {
      LOG.warn(
          "Expected to merge received {} with local topology {}, but failed to update local topology to {}. ",
          otherClusterTopology,
          currentTopology,
          newTopology,
          e);
    }
  }

  private void initialize(final BrokerCfg brokerCfg) throws IOException {
    persistedClusterTopology.initialize();
    if (persistedClusterTopology.getTopology() == null) {
      final var topology = initializeFromConfig(brokerCfg);
      persistedClusterTopology.update(topology);
      LOG.trace(
          "Initialized ClusterTopology from BrokerCfg {}", persistedClusterTopology.getTopology());
    }
  }

  private ClusterTopology initializeFromConfig(final BrokerCfg brokerCfg) throws IOException {
    final var partitionDistribution =
        new StaticPartitionDistributionResolver()
            .resolveTopology(brokerCfg.getExperimental().getPartitioning(), brokerCfg.getCluster());

    final var partitionsOwnedByMembers =
        partitionDistribution.partitions().stream()
            .flatMap(
                p ->
                    p.members().stream()
                        .map(m -> Map.entry(m, Map.entry(p.id().id(), p.getPriority(m)))))
            .collect(
                Collectors.groupingBy(
                    Entry::getKey,
                    Collectors.toMap(
                        e -> e.getValue().getKey(),
                        e -> PartitionState.active(e.getValue().getValue()))));

    final var memberStates =
        partitionsOwnedByMembers.entrySet().stream()
            .map(
                entry -> {
                  final MemberId memberId = entry.getKey();
                  final Map<Integer, PartitionState> partitionInfo = entry.getValue();
                  return Map.entry(memberId, MemberState.initializeAsActive(partitionInfo));
                })
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    final var topology = new ClusterTopology(0, memberStates, ClusterChangePlan.empty());

    updateLocalTopology(topology);
    return topology;
  }

  private void updateLocalTopology(final ClusterTopology topology) throws IOException {
    persistedClusterTopology.update(topology);
    gossipHandler.gossip(topology);
  }
}
