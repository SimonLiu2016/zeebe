/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.streamprocessor;

import static io.camunda.zeebe.engine.processing.streamprocessor.TypedEventRegistry.EVENT_REGISTRY;

import io.camunda.zeebe.logstreams.log.LogStreamBatchWriter;
import io.camunda.zeebe.logstreams.log.LogStreamBatchWriter.LogEntryBuilder;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.util.HashMap;
import java.util.Map;

public class LegacyTypedStreamWriterImpl implements LegacyTypedStreamWriter {

  private final Map<Class<? extends UnpackedObject>, ValueType> typeRegistry;
  private final RecordMetadata metadata = new RecordMetadata();
  private final LogStreamBatchWriter batchWriter;

  private long sourceRecordPosition = -1;

  public LegacyTypedStreamWriterImpl(final LogStreamBatchWriter batchWriter) {
    this.batchWriter = batchWriter;
    typeRegistry = new HashMap<>();
    EVENT_REGISTRY.forEach((e, c) -> typeRegistry.put(c, e));
  }

  protected void initMetadata(final RecordType type, final Intent intent, final RecordValue value) {
    metadata.reset();
    final ValueType valueType = typeRegistry.get(value.getClass());
    if (valueType == null) {
      // usually happens when the record is not registered at the TypedStreamEnvironment
      throw new RuntimeException("Missing value type mapping for record: " + value.getClass());
    }

    metadata.recordType(type).valueType(valueType).intent(intent);
  }

  protected void appendRecord(
      final long key, final RecordType type, final Intent intent, final RecordValue value) {
    appendRecord(key, type, intent, RejectionType.NULL_VAL, "", value);
  }

  @Override
  public void appendFollowUpCommand(final long key, final Intent intent, final RecordValue value) {
    appendRecord(key, RecordType.COMMAND, intent, value);
  }

  @Override
  public void appendRecord(
      final long key,
      final RecordType type,
      final Intent intent,
      final RejectionType rejectionType,
      final String rejectionReason,
      final RecordValue value) {

    final LogEntryBuilder event = batchWriter.event();

    if (sourceRecordPosition >= 0) {
      batchWriter.sourceRecordPosition(sourceRecordPosition);
    }

    initMetadata(type, intent, value);
    metadata.rejectionType(rejectionType);
    metadata.rejectionReason(rejectionReason);

    if (key >= 0) {
      event.key(key);
    } else {
      event.keyNull();
    }

    if (value instanceof BufferWriter) {
      event.metadataWriter(metadata).valueWriter((BufferWriter) value).done();
    } else {
      throw new RuntimeException(String.format("The record value %s is not a BufferWriter", value));
    }
  }

  @Override
  public void configureSourceContext(final long sourceRecordPosition) {
    this.sourceRecordPosition = sourceRecordPosition;
  }

  @Override
  public void reset() {
    sourceRecordPosition = -1;
    metadata.reset();
    batchWriter.reset();
  }

  @Override
  public long flush() {
    return batchWriter.tryWrite();
  }

  @Override
  public void appendFollowUpEvent(final long key, final Intent intent, final RecordValue value) {
    appendRecord(key, RecordType.EVENT, intent, value);
  }

  /**
   * Use this to know whether you can add an event of the given length to the underlying batch
   * writer.
   *
   * @param eventLength the length of the event that will be added to the batch
   * @return true if an event of length {@code eventLength} can be added to this batch such that it
   *     can later be written
   */
  @Override
  public boolean canWriteEventOfLength(final int eventLength) {
    return batchWriter.canWriteAdditionalEvent(eventLength);
  }
}
