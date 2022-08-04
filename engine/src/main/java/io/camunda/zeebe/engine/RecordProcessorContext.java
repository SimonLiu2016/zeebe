/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.engine.api.ProcessingScheduleService;
import io.camunda.zeebe.engine.api.StreamProcessorLifecycleAware;
import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessorListener;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.LegacyTypedStreamWriter;
import io.camunda.zeebe.engine.state.EventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class RecordProcessorContext {

  private final int partitionId;
  private final ProcessingScheduleService scheduleService;
  private final ZeebeDb zeebeDb;
  private final TransactionContext transactionContext;
  private final LegacyTypedStreamWriter streamWriter;
  private final LegacyTypedResponseWriter responseWriter;
  private final Function<MutableZeebeState, EventApplier> eventApplierFactory;
  private List<StreamProcessorLifecycleAware> lifecycleListeners = Collections.EMPTY_LIST;
  private StreamProcessorListener streamProcessorListener;

  public RecordProcessorContext(
      final int partitionId,
      final ProcessingScheduleService scheduleService,
      final ZeebeDb zeebeDb,
      final TransactionContext transactionContext,
      final LegacyTypedStreamWriter streamWriter,
      final LegacyTypedResponseWriter responseWriter,
      final Function<MutableZeebeState, EventApplier> eventApplierFactory) {
    this.partitionId = partitionId;
    this.scheduleService = scheduleService;
    this.zeebeDb = zeebeDb;
    this.transactionContext = transactionContext;
    this.streamWriter = streamWriter;
    this.responseWriter = responseWriter;
    this.eventApplierFactory = eventApplierFactory;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public ProcessingScheduleService getScheduleService() {
    return scheduleService;
  }

  public ZeebeDb getZeebeDb() {
    return zeebeDb;
  }

  public TransactionContext getTransactionContext() {
    return transactionContext;
  }

  public LegacyTypedStreamWriter getStreamWriterProxy() {
    return streamWriter;
  }

  public LegacyTypedResponseWriter getTypedResponseWriter() {
    return responseWriter;
  }

  public Function<MutableZeebeState, EventApplier> getEventApplierFactory() {
    return eventApplierFactory;
  }

  public List<StreamProcessorLifecycleAware> getLifecycleListeners() {
    return lifecycleListeners;
  }

  public void setLifecycleListeners(final List<StreamProcessorLifecycleAware> lifecycleListeners) {
    this.lifecycleListeners = lifecycleListeners;
  }

  public StreamProcessorListener getStreamProcessorListener() {
    return streamProcessorListener;
  }

  public void setStreamProcessorListener(final StreamProcessorListener streamProcessorListener) {
    this.streamProcessorListener = streamProcessorListener;
  }
}
