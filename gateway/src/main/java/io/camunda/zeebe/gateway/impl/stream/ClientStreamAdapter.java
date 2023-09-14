/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl.stream;

import static io.camunda.zeebe.gateway.RequestMapper.toJobActivationProperties;
import static io.camunda.zeebe.util.buffer.BufferUtil.wrapString;

import io.camunda.zeebe.gateway.ResponseMapper;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ActivatedJob;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.StreamActivatedJobsRequest;
import io.camunda.zeebe.protocol.impl.stream.job.ActivatedJobImpl;
import io.camunda.zeebe.protocol.impl.stream.job.JobActivationProperties;
import io.camunda.zeebe.scheduler.Actor;
import io.camunda.zeebe.scheduler.ConcurrencyControl;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.transport.stream.api.ClientStreamConsumer;
import io.camunda.zeebe.transport.stream.api.ClientStreamId;
import io.camunda.zeebe.transport.stream.api.ClientStreamer;
import io.camunda.zeebe.util.LockUtil;
import io.camunda.zeebe.util.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
import org.agrona.DirectBuffer;

public class ClientStreamAdapter extends Actor {
  private final ClientStreamer<JobActivationProperties> jobStreamer;

  public ClientStreamAdapter(final ClientStreamer<JobActivationProperties> jobStreamer) {
    this.jobStreamer = jobStreamer;
  }

  public void handle(
      final StreamActivatedJobsRequest request,
      final ServerCallStreamObserver<ActivatedJob> responseObserver) {
    if (request.getType().isBlank()) {
      handleError(responseObserver, "type", "present", "blank");
      return;
    }
    if (request.getTimeout() < 1) {
      handleError(
          responseObserver, "timeout", "greater than zero", Long.toString(request.getTimeout()));
      return;
    }

    handleInternal(request, responseObserver);
  }

  private void handleInternal(
      final StreamActivatedJobsRequest request,
      final ServerCallStreamObserver<ActivatedJob> responseObserver) {
    final var jobActivationProperties = toJobActivationProperties(request);
    final var streamType = wrapString(request.getType());
    final var consumer = new ClientStreamConsumerImpl(responseObserver, actor);
    final var cleaner = new AsyncJobStreamRemover(jobStreamer);

    // setting the handlers has to be done before the call is started, so we cannot do it in the
    // actor callbacks, which is why the remover can handle being called out of order
    responseObserver.setOnCloseHandler(cleaner);
    responseObserver.setOnCancelHandler(cleaner);

    actor.runOnCompletion(
        jobStreamer.add(streamType, jobActivationProperties, consumer), cleaner::onStreamAdded);
  }

  private void handleError(
      final ServerCallStreamObserver<ActivatedJob> responseObserver,
      final String field,
      final String expectation,
      final String actual) {
    final var format = "Expected to stream activated jobs with %s to be %s, but it was %s";
    final String errorMessage = format.formatted(field, expectation, actual);
    responseObserver.onError(
        new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(errorMessage)));
  }

  @VisibleForTesting("Allow unit testing behavior job handling behavior")
  static final class ClientStreamConsumerImpl implements ClientStreamConsumer {
    private final StreamObserver<ActivatedJob> responseObserver;
    private final ConcurrencyControl executor;

    ClientStreamConsumerImpl(
        final StreamObserver<ActivatedJob> responseObserver, final ConcurrencyControl executor) {
      this.responseObserver = responseObserver;
      this.executor = executor;
    }

    @Override
    public ActorFuture<Void> push(final DirectBuffer payload) {
      try {
        return executor.call(
            () -> {
              handlePushedJob(payload);
              return null;
            });
      } catch (final Exception e) {
        // in case the actor is closed
        responseObserver.onError(e);
        return CompletableActorFuture.completedExceptionally(e);
      }
    }

    private void handlePushedJob(final DirectBuffer payload) {
      final ActivatedJobImpl deserializedJob = new ActivatedJobImpl();
      deserializedJob.wrap(payload);
      final ActivatedJob activatedJob = ResponseMapper.toActivatedJob(deserializedJob);
      try {
        responseObserver.onNext(activatedJob);
      } catch (final Exception e) {
        responseObserver.onError(e);
        throw e;
      }
    }
  }

  private static final class AsyncJobStreamRemover implements Runnable {
    private final Lock lock = new ReentrantLock();
    private final ClientStreamer<JobActivationProperties> jobStreamer;

    @GuardedBy("lock")
    private boolean isRemoved;

    @GuardedBy("lock")
    private ClientStreamId streamId;

    private AsyncJobStreamRemover(final ClientStreamer<JobActivationProperties> jobStreamer) {
      this.jobStreamer = jobStreamer;
    }

    @Override
    public void run() {
      LockUtil.withLock(lock, this::lockedRemove);
    }

    private void onStreamAdded(final ClientStreamId streamId, final Throwable error) {
      LockUtil.withLock(lock, () -> lockedOnStreamAdded(streamId));
    }

    @GuardedBy("lock")
    private void lockedRemove() {
      isRemoved = true;

      if (streamId != null) {
        jobStreamer.remove(streamId);
      }
    }

    @GuardedBy("lock")
    private void lockedOnStreamAdded(final ClientStreamId streamId) {
      if (isRemoved) {
        jobStreamer.remove(streamId);
        return;
      }

      this.streamId = streamId;
    }
  }
}
