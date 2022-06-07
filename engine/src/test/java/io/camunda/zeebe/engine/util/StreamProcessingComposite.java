/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.util;

import static io.camunda.zeebe.engine.util.Records.processInstance;

import io.camunda.zeebe.db.ZeebeDbFactory;
import io.camunda.zeebe.engine.processing.streamprocessor.ReadonlyProcessingContext;
import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorFactory;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessors;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.camunda.zeebe.engine.state.immutable.LastProcessedPositionState;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.logstreams.log.LogStreamBatchWriter;
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.util.sched.Actor;
import io.camunda.zeebe.util.sched.ActorScheduler;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class StreamProcessingComposite {

  private static final String STREAM_NAME = "stream-";

  private final TestStreams streams;
  private final int partitionId;
  private final ZeebeDbFactory<?> zeebeDbFactory;
  private MutableZeebeState zeebeState;
  private LastProcessedPositionState lastProcessedPositionState;

  private final WriteActor writeActor = new WriteActor();

  public StreamProcessingComposite(
      final TestStreams streams,
      final int partitionId,
      final ZeebeDbFactory<?> zeebeDbFactory,
      final ActorScheduler actorScheduler) {
    this.streams = streams;
    this.partitionId = partitionId;
    this.zeebeDbFactory = zeebeDbFactory;
    actorScheduler.submitActor(writeActor);
  }

  public LogStreamRecordWriter getLogStreamRecordWriter(final int partitionId) {
    final String logName = getLogName(partitionId);
    return streams.getLogStreamRecordWriter(logName);
  }

  public StreamProcessor startTypedStreamProcessor(final StreamProcessorTestFactory factory) {
    return startTypedStreamProcessor(
        (processingContext) -> createTypedRecordProcessors(factory, processingContext));
  }

  private TypedRecordProcessors createTypedRecordProcessors(
      final StreamProcessorTestFactory factory,
      final io.camunda.zeebe.engine.processing.streamprocessor.ProcessingContext
          processingContext) {
    zeebeState = processingContext.getZeebeState();
    lastProcessedPositionState = processingContext.getLastProcessedPositionState();
    return factory.build(
        TypedRecordProcessors.processors(
            zeebeState.getKeyGenerator(), processingContext.getWriters()),
        processingContext);
  }

  public StreamProcessor startTypedStreamProcessor(final TypedRecordProcessorFactory factory) {
    return startTypedStreamProcessor(partitionId, factory);
  }

  public StreamProcessor startTypedStreamProcessor(
      final int partitionId, final TypedRecordProcessorFactory factory) {
    return streams.startStreamProcessor(
        getLogName(partitionId),
        zeebeDbFactory,
        (processingContext -> {
          zeebeState = processingContext.getZeebeState();
          lastProcessedPositionState = processingContext.getLastProcessedPositionState();
          return factory.createProcessors(processingContext);
        }));
  }

  public StreamProcessor startTypedStreamProcessorNotAwaitOpening(
      final StreamProcessorTestFactory factory) {
    return startTypedStreamProcessorNotAwaitOpening(
        (processingContext) -> createTypedRecordProcessors(factory, processingContext));
  }

  public StreamProcessor startTypedStreamProcessorNotAwaitOpening(
      final TypedRecordProcessorFactory factory) {
    return startTypedStreamProcessorNotAwaitOpening(partitionId, factory);
  }

  public StreamProcessor startTypedStreamProcessorNotAwaitOpening(
      final int partitionId, final TypedRecordProcessorFactory factory) {
    return streams.startStreamProcessorNotAwaitOpening(
        getLogName(partitionId),
        zeebeDbFactory,
        (processingContext -> {
          zeebeState = processingContext.getZeebeState();
          lastProcessedPositionState = processingContext.getLastProcessedPositionState();
          return factory.createProcessors(processingContext);
        }));
  }

  public StreamProcessor startTypedStreamProcessorNotAwaitOpening(
      final int partitionId,
      final TypedRecordProcessorFactory factory,
      final Function<LogStreamBatchWriter, TypedStreamWriter> streamWriterFactory) {
    return streams.startStreamProcessorNotAwaitOpening(
        getLogName(partitionId),
        zeebeDbFactory,
        (processingContext -> {
          zeebeState = processingContext.getZeebeState();
          lastProcessedPositionState = processingContext.getLastProcessedPositionState();
          return factory.createProcessors(processingContext);
        }),
        streamWriterFactory);
  }

  public void pauseProcessing(final int partitionId) {
    streams.pauseProcessing(getLogName(partitionId));
  }

  public void resumeProcessing(final int partitionId) {
    streams.resumeProcessing(getLogName(partitionId));
  }

  public void snapshot(final int partitionId) {
    streams.snapshot(getLogName(partitionId));
  }

  public void closeStreamProcessor(final int partitionId) {
    try {
      streams.closeProcessor(getLogName(partitionId));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public StreamProcessor getStreamProcessor(final int partitionId) {
    return streams.getStreamProcessor(getLogName(partitionId));
  }

  public MutableZeebeState getZeebeState() {
    return zeebeState;
  }

  public long getLastSuccessfulProcessedRecordPosition() {
    return lastProcessedPositionState.getLastSuccessfulProcessedRecordPosition();
  }

  public RecordStream events() {
    return new RecordStream(streams.events(getLogName(partitionId)));
  }

  public long writeProcessInstanceEventWithSource(
      final ProcessInstanceIntent intent, final int instanceKey, final long sourceEventPosition) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .event(processInstance(instanceKey))
            .recordType(RecordType.EVENT)
            .sourceRecordPosition(sourceEventPosition)
            .intent(intent);
    return writeActor.submit(writer::write).join();
  }

  public long writeProcessInstanceEvent(final ProcessInstanceIntent intent, final int instanceKey) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .event(processInstance(instanceKey))
            .recordType(RecordType.EVENT)
            .intent(intent);
    return writeActor.submit(writer::write).join();
  }

  public long writeEvent(final long key, final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .recordType(RecordType.EVENT)
            .key(key)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeEvent(final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .recordType(RecordType.EVENT)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeBatch(final RecordToWrite... recordsToWrite) {
    final var batchWriter = streams.setupBatchWriter(getLogName(partitionId), recordsToWrite);
    return writeActor.submit(batchWriter::tryWrite).join();
  }

  public long writeCommandOnPartition(
      final int partition, final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partition))
            .recordType(RecordType.COMMAND)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeCommandOnPartition(
      final int partition, final long key, final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partition))
            .key(key)
            .recordType(RecordType.COMMAND)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeCommand(final long key, final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .recordType(RecordType.COMMAND)
            .key(key)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeCommand(final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .recordType(RecordType.COMMAND)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeCommand(
      final int requestStreamId,
      final long requestId,
      final Intent intent,
      final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .recordType(RecordType.COMMAND)
            .requestId(requestId)
            .requestStreamId(requestStreamId)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public long writeCommandRejection(final Intent intent, final UnpackedObject value) {
    final var writer =
        streams
            .newRecord(getLogName(partitionId))
            .recordType(RecordType.COMMAND_REJECTION)
            .intent(intent)
            .event(value);
    return writeActor.submit(writer::write).join();
  }

  public static String getLogName(final int partitionId) {
    return STREAM_NAME + partitionId;
  }

  /** Used to run writes within an actor thread. */
  private static final class WriteActor extends Actor {
    public ActorFuture<Long> submit(final Callable<Long> write) {
      return executionContext.call(write);
    }
  }

  @FunctionalInterface
  public interface StreamProcessorTestFactory {
    TypedRecordProcessors build(
        TypedRecordProcessors builder, ReadonlyProcessingContext processingContext);
  }
}
