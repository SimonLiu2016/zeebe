/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDbTransaction;
import io.camunda.zeebe.engine.metrics.StreamProcessorMetrics;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.camunda.zeebe.engine.state.mutable.MutableLastProcessedPositionState;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.logstreams.impl.Loggers;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.logstreams.log.LogStreamReader;
import io.camunda.zeebe.logstreams.log.LoggedEvent;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.value.error.ErrorRecord;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.ErrorIntent;
import io.camunda.zeebe.util.exception.RecoverableException;
import io.camunda.zeebe.util.exception.UnrecoverableException;
import io.camunda.zeebe.util.retry.AbortableRetryStrategy;
import io.camunda.zeebe.util.retry.RecoverableRetryStrategy;
import io.camunda.zeebe.util.retry.RetryStrategy;
import io.camunda.zeebe.util.sched.ActorControl;
import io.camunda.zeebe.util.sched.clock.ActorClock;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.prometheus.client.Histogram;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Represents the processing state machine, which is executed on normal processing.
 *
 * <pre>
 *
 * +------------------+            +--------------------+
 * |                  |            |                    |      exception
 * | readNextRecord() |----------->|  processCommand()  |------------------+
 * |                  |            |                    |                  v
 * +------------------+            +--------------------+            +---------------+
 *           ^                             |                         |               |------+
 *           |                             |         +-------------->|   onError()   |      | exception
 *           |                             |         |  exception    |               |<-----+
 *           |                     +-------v-------------+           +---------------+
 *           |                     |                     |                 |
 *           |                     |   writeRecords()    |                 |
 *           |                     |                     |<----------------+
 * +----------------------+        +---------------------+
 * |                      |                 |
 * | executeSideEffects() |                 v
 * |                      |       +----------------------+
 * +----------------------+       |                      |
 *           ^                    |     updateState()    |
 *           +--------------------|                      |
 *                                +----------------------+
 *                                       ^      |
 *                                       |      | exception
 *                                       |      |
 *                                    +---------v----+
 *                                    |              |
 *                                    |   onError()  |
 *                                    |              |
 *                                    +--------------+
 *                                       ^     |
 *                                       |     |  exception
 *                                       +-----+
 *
 * </pre>
 */
public final class ProcessingStateMachine {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;
  private static final String ERROR_MESSAGE_WRITE_RECORD_ABORTED =
      "Expected to write one or more follow-up records for record '{} {}' without errors, but exception was thrown.";
  private static final String ERROR_MESSAGE_ROLLBACK_ABORTED =
      "Expected to roll back the current transaction for record '{} {}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED =
      "Expected to execute side effects for record '{} {}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_UPDATE_STATE_FAILED =
      "Expected to successfully update state for record '{} {}', but caught an exception. Retry.";
  private static final String ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT =
      "Expected to find processor for record '{} {}', but caught an exception. Skip this record.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT =
      "Expected to successfully process record '{} {}' with processor, but caught an exception. Skip this record.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING =
      "Expected to process record '{} {}' successfully on stream processor, but caught recoverable exception. Retry processing.";
  private static final String PROCESSING_ERROR_MESSAGE =
      "Expected to process record '%s' without errors, but exception occurred with message '%s'.";
  private static final String NOTIFY_PROCESSED_LISTENER_ERROR_MESSAGE =
      "Expected to invoke processed listener for record {} successfully, but exception was thrown.";
  private static final String NOTIFY_SKIPPED_LISTENER_ERROR_MESSAGE =
      "Expected to invoke skipped listener for record '{} {}' successfully, but exception was thrown.";

  private static final Duration PROCESSING_RETRY_DELAY = Duration.ofMillis(250);

  private static final MetadataFilter PROCESSING_FILTER =
      recordMetadata -> recordMetadata.getRecordType() == RecordType.COMMAND;

  private final EventFilter eventFilter =
      new MetadataEventFilter(new RecordProtocolVersionFilter().and(PROCESSING_FILTER));

  private final EventFilter commandFilter =
      new MetadataEventFilter(
          recordMetadata -> recordMetadata.getRecordType() != RecordType.COMMAND);

  private final MutableZeebeState zeebeState;
  private final MutableLastProcessedPositionState lastProcessedPositionState;
  private final RecordMetadata metadata = new RecordMetadata();
  private final TypedResponseWriter responseWriter;
  private final ActorControl actor;
  private final LogStream logStream;
  private final LogStreamReader logStreamReader;
  private final TypedStreamWriter logStreamWriter;
  private final TransactionContext transactionContext;
  private final RetryStrategy writeRetryStrategy;
  private final RetryStrategy sideEffectsRetryStrategy;
  private final RetryStrategy updateStateRetryStrategy;
  private final BooleanSupplier shouldProcessNext;
  private final BooleanSupplier abortCondition;
  private final ErrorRecord errorRecord = new ErrorRecord();
  private final RecordValues recordValues;
  private final RecordProcessorMap recordProcessorMap;
  private final TypedEventImpl typedCommand;
  private final StreamProcessorMetrics metrics;
  private final StreamProcessorListener streamProcessorListener;

  // current iteration
  private SideEffectProducer sideEffectProducer;
  private LoggedEvent currentRecord;
  private TypedRecordProcessor<?> currentProcessor;
  private ZeebeDbTransaction zeebeDbTransaction;
  private long writtenPosition = StreamProcessor.UNSET_POSITION;
  private long lastSuccessfulProcessedRecordPosition = StreamProcessor.UNSET_POSITION;
  private long lastWrittenPosition = StreamProcessor.UNSET_POSITION;
  private volatile boolean onErrorHandlingLoop;
  private int onErrorRetries;
  // Used for processing duration metrics
  private Histogram.Timer processingTimer;
  private boolean reachedEnd = true;
  private final Tracer tracer;
  private Span currentProcessSpan;
  private Scope currentScope;

  public ProcessingStateMachine(
      final ProcessingContext context, final BooleanSupplier shouldProcessNext) {

    actor = context.getActor();
    recordProcessorMap = context.getRecordProcessorMap();
    recordValues = context.getRecordValues();
    logStreamReader = context.getLogStreamReader();
    logStreamWriter = context.getLogStreamWriter();
    logStream = context.getLogStream();
    zeebeState = context.getZeebeState();
    transactionContext = context.getTransactionContext();
    abortCondition = context.getAbortCondition();
    lastProcessedPositionState = context.getLastProcessedPositionState();
    tracer =
        StreamProcessor.openTelemetrySdk
            .getTracerProvider()
            .get((ProcessingStateMachine.class.getName()));
    writeRetryStrategy = new AbortableRetryStrategy(actor);
    sideEffectsRetryStrategy = new AbortableRetryStrategy(actor);
    updateStateRetryStrategy = new RecoverableRetryStrategy(actor);
    this.shouldProcessNext = shouldProcessNext;

    final int partitionId = logStream.getPartitionId();
    typedCommand = new TypedEventImpl(partitionId);
    responseWriter = context.getWriters().response();

    metrics = new StreamProcessorMetrics(partitionId);
    streamProcessorListener = context.getStreamProcessorListener();
  }

  private void skipRecord() {
    notifySkippedListener(currentRecord);
    actor.submit(this::readNextRecord);
    metrics.eventSkipped();
  }

  void readNextRecord() {
    if (onErrorRetries > 0) {
      onErrorHandlingLoop = false;
      onErrorRetries = 0;
    }

    tryToReadNextRecord();
  }

  private void tryToReadNextRecord() {
    final var hasNext = logStreamReader.hasNext();

    if (currentRecord != null) {
      final var previousRecord = currentRecord;
      // All commands cause a follow-up event or rejection, which means the processor
      // reached the end of the log if:
      //  * the last record was an event or rejection
      //  * and there is no next record on the log
      //  * and this was the last record written (records that have been written to the dispatcher
      //    might not be written to the log yet, which means they will appear shortly after this)
      reachedEnd =
          commandFilter.applies(previousRecord)
              && !hasNext
              && lastWrittenPosition <= previousRecord.getPosition();
    }

    if (shouldProcessNext.getAsBoolean() && hasNext && currentProcessor == null) {
      currentRecord = logStreamReader.next();

      if (eventFilter.applies(currentRecord)) {
        processCommand(currentRecord);
      } else {
        skipRecord();
      }
    }
  }

  /**
   * Be aware this is a transient property which can change anytime, e.g. if a new command is
   * written to the log.
   *
   * @return true if the ProcessingStateMachine has reached the end of the log and nothing is left
   *     to being processed/applied, false otherwise
   */
  public boolean hasReachedEnd() {
    return reachedEnd;
  }

  private void processCommand(final LoggedEvent command) {
    if (currentProcessSpan != null) {
      if (currentScope != null) {
        currentScope.close();
      }
      currentProcessSpan.setAttribute("key", currentRecord.getKey()).end();
    }
    metadata.reset();
    command.readMetadata(metadata);

    currentProcessor = chooseNextProcessor(command);
    if (currentProcessor == null) {
      skipRecord();
      return;
    }

    currentProcessSpan = tracer.spanBuilder("processInTransaction").setNoParent().startSpan();
    currentScope = currentProcessSpan.makeCurrent();

    // Here we need to get the current time, since we want to calculate
    // how long it took between writing to the dispatcher and processing.
    // In all other cases we should prefer to use the Prometheus Timer API.
    final var processingStartTime = ActorClock.currentTimeMillis();
    processingTimer = metrics.startProcessingDurationTimer(metadata.getRecordType());

    try {
      final var value = recordValues.readRecordValue(command, metadata.getValueType());
      typedCommand.wrap(command, metadata, value);

      metrics.processingLatency(command.getTimestamp(), processingStartTime);

      final var span =
          tracer
              .spanBuilder("processInTransaction")
              .setAttribute("key", typedCommand.getKey())
              .setAttribute("intent", typedCommand.getIntent().name())
              .setAttribute("recordType", typedCommand.getRecordType().name())
              .setAttribute("valueType", typedCommand.getValueType().name())
              .setAttribute("partition", typedCommand.getPartitionId())
              .startSpan();

      try (final var scope = span.makeCurrent()) {
        processInTransaction(typedCommand);
      } finally {
        span.end();
      }

      metrics.commandsProcessed();

      writeRecords();
    } catch (final RecoverableException recoverableException) {
      // recoverable
      LOG.error(
          ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING,
          command,
          metadata,
          recoverableException);
      actor.runDelayed(PROCESSING_RETRY_DELAY, () -> processCommand(currentRecord));
    } catch (final UnrecoverableException unrecoverableException) {
      throw unrecoverableException;
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT, command, metadata, e);
      onError(e, this::writeRecords);
    }
  }

  private TypedRecordProcessor<?> chooseNextProcessor(final LoggedEvent command) {
    TypedRecordProcessor<?> typedRecordProcessor = null;

    try {
      typedRecordProcessor =
          recordProcessorMap.get(
              metadata.getRecordType(), metadata.getValueType(), metadata.getIntent().value());
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT, command, metadata, e);
    }

    return typedRecordProcessor;
  }

  private void processInTransaction(final TypedEventImpl typedRecord) throws Exception {
    zeebeDbTransaction = transactionContext.getCurrentTransaction();
    zeebeDbTransaction.run(
        () -> {
          final long position = typedRecord.getPosition();
          resetOutput(position);

          // default side effect is responses; can be changed by processor
          sideEffectProducer = responseWriter;
          final boolean isNotOnBlacklist =
              !zeebeState.getBlackListState().isOnBlacklist(typedRecord);
          if (isNotOnBlacklist) {
            currentProcessor.processRecord(
                position,
                typedRecord,
                responseWriter,
                logStreamWriter,
                this::setSideEffectProducer);
          }
          lastProcessedPositionState.markAsProcessed(position);
        });
  }

  private void resetOutput(final long sourceRecordPosition) {
    responseWriter.reset();
    logStreamWriter.reset();
    logStreamWriter.configureSourceContext(sourceRecordPosition);
  }

  public void setSideEffectProducer(final SideEffectProducer sideEffectProducer) {
    this.sideEffectProducer = sideEffectProducer;
  }

  private void onError(final Throwable processingException, final Runnable nextStep) {
    onErrorRetries++;
    if (onErrorRetries > 1) {
      onErrorHandlingLoop = true;
    }
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.rollback();
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_ROLLBACK_ABORTED, currentRecord, metadata, throwable);
          }
          try {
            errorHandlingInTransaction(processingException);

            nextStep.run();
          } catch (final Exception ex) {
            onError(ex, nextStep);
          }
        });
  }

  private void errorHandlingInTransaction(final Throwable processingException) throws Exception {
    zeebeDbTransaction = transactionContext.getCurrentTransaction();
    zeebeDbTransaction.run(
        () -> {
          final long position = typedCommand.getPosition();
          resetOutput(position);

          writeRejectionOnCommand(processingException);
          errorRecord.initErrorRecord(processingException, position);

          zeebeState
              .getBlackListState()
              .tryToBlacklist(typedCommand, errorRecord::setProcessInstanceKey);

          logStreamWriter.appendFollowUpEvent(
              typedCommand.getKey(), ErrorIntent.CREATED, errorRecord);
        });
  }

  private void writeRejectionOnCommand(final Throwable exception) {
    final String errorMessage =
        String.format(PROCESSING_ERROR_MESSAGE, typedCommand, exception.getMessage());
    LOG.error(errorMessage, exception);

    logStreamWriter.appendRejection(typedCommand, RejectionType.PROCESSING_ERROR, errorMessage);
    responseWriter.writeRejectionOnCommand(
        typedCommand, RejectionType.PROCESSING_ERROR, errorMessage);
  }

  private void writeRecords() {
    final var span =
        tracer.spanBuilder("writeRecords").setAttribute("key", currentRecord.getKey()).startSpan();

    final var scope = span.makeCurrent();
    final ActorFuture<Boolean> retryFuture =
        writeRetryStrategy.runWithRetry(
            () -> {
              final long position = logStreamWriter.flush();

              // only overwrite position if records were flushed
              if (position > 0) {
                writtenPosition = position;
              }

              return position >= 0;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, t) -> {
          scope.close();
          span.end();
          if (t != null) {
            LOG.error(ERROR_MESSAGE_WRITE_RECORD_ABORTED, currentRecord, metadata, t);
            onError(t, this::writeRecords);
          } else {
            // We write various type of records. The positions are always increasing and
            // incremented by 1 for one record (even in a batch), so we can count the amount
            // of written records via the lastWritten and now written position.
            final var amount = writtenPosition - lastWrittenPosition;
            metrics.recordsWritten(amount);
            updateState();
          }
        });
  }

  private void updateState() {
    final var span =
        tracer
            .spanBuilder("commitTransaction")
            .setAttribute("key", currentRecord.getKey())
            .startSpan();

    final var scope = span.makeCurrent();
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.commit();
              lastSuccessfulProcessedRecordPosition = currentRecord.getPosition();
              metrics.setLastProcessedPosition(lastSuccessfulProcessedRecordPosition);
              lastWrittenPosition = writtenPosition;

              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          scope.close();
          span.end();
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_UPDATE_STATE_FAILED, currentRecord, metadata, throwable);
            onError(throwable, this::updateState);
          } else {
            executeSideEffects();
          }
        });
  }

  private void executeSideEffects() {
    final var span =
        tracer.spanBuilder("sideEffects").setAttribute("key", currentRecord.getKey()).startSpan();

    final var scope = span.makeCurrent();
    final ActorFuture<Boolean> retryFuture =
        sideEffectsRetryStrategy.runWithRetry(sideEffectProducer::flush, abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(
                ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED, currentRecord, metadata, throwable);
          }

          notifyProcessedListener(typedCommand);

          // observe the processing duration
          processingTimer.close();

          // continue with next record
          currentProcessor = null;

          scope.close();
          span.end();
          actor.submit(this::readNextRecord);
        });
  }

  private void notifyProcessedListener(final TypedRecord processedRecord) {
    try {
      streamProcessorListener.onProcessed(processedRecord);
    } catch (final Exception e) {
      LOG.error(NOTIFY_PROCESSED_LISTENER_ERROR_MESSAGE, processedRecord, e);
    }
  }

  private void notifySkippedListener(final LoggedEvent skippedRecord) {
    try {
      streamProcessorListener.onSkipped(skippedRecord);
    } catch (final Exception e) {
      LOG.error(NOTIFY_SKIPPED_LISTENER_ERROR_MESSAGE, skippedRecord, metadata, e);
    }
  }

  public long getLastSuccessfulProcessedRecordPosition() {
    return lastSuccessfulProcessedRecordPosition;
  }

  public long getLastWrittenPosition() {
    return lastWrittenPosition;
  }

  public boolean isMakingProgress() {
    return !onErrorHandlingLoop;
  }

  public void startProcessing(final LastProcessingPositions lastProcessingPositions) {
    // Replay ends at the end of the log and returns the lastSourceRecordPosition
    // which is equal to the last processed position
    // we need to seek to the next record after that position where the processing should start
    // Be aware on processing we ignore events, so we will process the next command
    final var lastProcessedPosition = lastProcessingPositions.getLastProcessedPosition();
    logStreamReader.seekToNextEvent(lastProcessedPosition);
    if (lastSuccessfulProcessedRecordPosition == StreamProcessor.UNSET_POSITION) {
      lastSuccessfulProcessedRecordPosition = lastProcessedPosition;
    }

    if (lastWrittenPosition == StreamProcessor.UNSET_POSITION) {
      lastWrittenPosition = lastProcessingPositions.getLastWrittenPosition();
    }

    actor.submit(this::readNextRecord);
  }
}
