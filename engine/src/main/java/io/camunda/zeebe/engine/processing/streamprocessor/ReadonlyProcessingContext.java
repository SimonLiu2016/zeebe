/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.EventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.logstreams.log.LogStreamReader;
import io.camunda.zeebe.util.sched.ActorContext;
import java.util.function.BooleanSupplier;

public interface ReadonlyProcessingContext {

  /**
   * @return the actor on which the processing runs
   */
  ActorContext getActor();

  /**
   * @return the logstream, on which the processor runs
   */
  LogStream getLogStream();

  /**
   * @return the reader, which is used by the processor to read next events
   */
  LogStreamReader getLogStreamReader();

  /**
   * @return the maximum fragment size we can write and read this contains the record metadata and
   *     record value etc.
   */
  int getMaxFragmentSize();

  /**
   * @return the actual log stream writer, used to write any record
   */
  TypedStreamWriter getLogStreamWriter();

  /**
   * @return the specific writers, like command, response, etc
   */
  Writers getWriters();

  /**
   * @return the pool, which contains the mapping from ValueType to UnpackedObject (record)
   */
  RecordValues getRecordValues();

  /**
   * @return the map of processors, which are executed during processing
   */
  RecordProcessorMap getRecordProcessorMap();

  /**
   * @return the state, where the data is stored during processing
   */
  MutableZeebeState getZeebeState();

  /**
   * @return the transaction context for the current actor
   */
  TransactionContext getTransactionContext();

  /**
   * @return condition which indicates, whether the processing should stop or not
   */
  BooleanSupplier getAbortCondition();

  /**
   * @return the consumer of events to apply their state changes
   */
  EventApplier getEventApplier();
}
