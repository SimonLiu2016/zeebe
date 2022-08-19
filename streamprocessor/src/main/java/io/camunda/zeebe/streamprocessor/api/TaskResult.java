/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.streamprocessor.api;

import io.camunda.zeebe.logstreams.log.LogStreamBatchWriter;

/** Here the interface is just a suggestion. Can be whatever PDT team thinks is best to work with */
public interface TaskResult {

  long writeRecordsToStream(LogStreamBatchWriter logStreamBatchWriter);
}
