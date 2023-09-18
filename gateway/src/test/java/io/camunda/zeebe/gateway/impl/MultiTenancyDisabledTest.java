/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl;

import static io.camunda.zeebe.gateway.api.util.GatewayAssertions.statusRuntimeExceptionWithStatusCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.camunda.identity.sdk.tenants.dto.Tenant;
import io.camunda.zeebe.auth.impl.Authorization;
import io.camunda.zeebe.gateway.api.deployment.DeployResourceStub;
import io.camunda.zeebe.gateway.api.job.ActivateJobsStub;
import io.camunda.zeebe.gateway.api.process.CreateProcessInstanceStub;
import io.camunda.zeebe.gateway.api.util.GatewayTest;
import io.camunda.zeebe.gateway.impl.broker.request.BrokerExecuteCommand;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsRequest;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsResponse;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ActivatedJob;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.CreateProcessInstanceRequest;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.CreateProcessInstanceResponse;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DecisionMetadata;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DecisionRequirementsMetadata;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployResourceRequest;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployResourceRequest.Builder;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployResourceResponse;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.ProcessMetadata;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import io.grpc.Status;
import java.util.Iterator;
import java.util.List;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;

public class MultiTenancyDisabledTest extends GatewayTest {

  public MultiTenancyDisabledTest() {
    super(cfg -> cfg.getMultiTenancy().setEnabled(false));
  }

  @Before
  public void setup() {
    new DeployResourceStub().registerWith(brokerClient);
    new CreateProcessInstanceStub().registerWith(brokerClient);
    new ActivateJobsStub().registerWith(brokerClient);
  }

  private void assertThatDefaultTenantIdSet() {
    final var brokerRequest = brokerClient.getSingleBrokerRequest();
    assertThat(((BrokerExecuteCommand<?>) brokerRequest).getAuthorization().toDecodedMap())
        .describedAs("The broker request should contain the default tenant as authorized tenant")
        .hasEntrySatisfying(
            Authorization.AUTHORIZED_TENANTS,
            v -> assertThat(v).asList().contains(TenantOwned.DEFAULT_TENANT_IDENTIFIER));

    assumeThat(brokerRequest.getRequestWriter())
        .describedAs(
            "The rest of this assertion only makes sense when the broker request contains a record that is TenantOwned")
        .isInstanceOf(TenantOwned.class);
    assertThat(((TenantOwned) brokerRequest.getRequestWriter()).getTenantId())
        .describedAs("The tenant id should be set to the default tenant")
        .isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);
  }

  private void assertThatRejectsRequest(final ThrowingCallable requestCallable, final String name) {
    assertThatThrownBy(requestCallable)
        .is(statusRuntimeExceptionWithStatusCode(Status.INVALID_ARGUMENT.getCode()))
        .hasMessageContaining("Expected to handle gRPC request " + name + " with tenant")
        .hasMessageContaining("but multi-tenancy is disabled");
  }

  @Test
  public void deployResourceRequestShouldContainDefaultTenantAsAuthorizedTenants() {
    // given
    final var request = DeployResourceRequest.newBuilder().build();

    // when
    final var response = client.deployResource(request);
    assertThat(response).isNotNull();

    // then
    assertThatDefaultTenantIdSet();
  }

  @Test
  public void deployResourceRequestRejectsTenantId() {
    // given
    final var request = DeployResourceRequest.newBuilder().setTenantId("tenant-a").build();

    // when/then
    assertThatRejectsRequest(() -> client.deployResource(request), "DeployResource");
  }

  @Test
  public void deployResourceResponseHasTenantId() {
    // given
    when(gateway.getIdentityMock().tenants().forToken(anyString()))
        .thenReturn(List.of(new Tenant("tenant-a", "A"), new Tenant("tenant-b", "B")));

    // when
    final Builder requestBuilder = DeployResourceRequest.newBuilder();
    requestBuilder
        .addResourcesBuilder()
        .setName("testProcess.bpmn")
        .setContent(ByteString.copyFromUtf8("<xml/>"));
    requestBuilder
        .addResourcesBuilder()
        .setName("testDecision.dmn")
        .setContent(ByteString.copyFromUtf8("test"));
    final DeployResourceResponse response = client.deployResource(requestBuilder.build());
    assertThat(response).isNotNull();

    // then
    assertThat(response.getTenantId()).isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);

    assumeThat(response.getDeploymentsCount())
        .describedAs("Any metadata of the deployed resources should also contain the tenant id")
        .isEqualTo(3);
    final ProcessMetadata process = response.getDeployments(0).getProcess();
    assertThat(process.getTenantId()).isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);

    final DecisionMetadata decision = response.getDeployments(1).getDecision();
    assertThat(decision.getTenantId()).isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);

    final DecisionRequirementsMetadata drg = response.getDeployments(2).getDecisionRequirements();
    assertThat(drg.getTenantId()).isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);
  }

  @Test
  public void createProcessInstanceRequestShouldContainDefaultTenantAsAuthorizedTenants() {
    // given
    final var request = CreateProcessInstanceRequest.newBuilder().build();

    // when
    final var response = client.createProcessInstance(request);
    assertThat(response).isNotNull();

    // then
    assertThatDefaultTenantIdSet();
  }

  @Test
  public void createProcessInstanceRequestRejectsTenantId() {
    // given
    final var request = CreateProcessInstanceRequest.newBuilder().setTenantId("tenant-a").build();

    // when/then
    assertThatRejectsRequest(() -> client.createProcessInstance(request), "CreateProcessInstance");
  }

  @Test
  public void createProcessInstanceResponseHasTenantId() {
    // given
    when(gateway.getIdentityMock().tenants().forToken(anyString()))
        .thenReturn(List.of(new Tenant("tenant-a", "A"), new Tenant("tenant-b", "B")));

    // when
    final CreateProcessInstanceResponse response =
        client.createProcessInstance(CreateProcessInstanceRequest.newBuilder().build());
    assertThat(response).isNotNull();

    // then
    assertThat(response.getTenantId()).isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);
  }

  @Test
  public void activateJobsRequestShouldContainDefaultTenantAsAuthorizedTenants() {
    // given
    final var request = ActivateJobsRequest.newBuilder().build();

    // when
    final var response = client.activateJobs(request);
    assertThat(response.hasNext()).isTrue();

    // then
    assertThatDefaultTenantIdSet();
  }

  @Test
  public void activateJobsRequestRejectsTenantId() {
    // given
    final var request = ActivateJobsRequest.newBuilder().addTenantIds("tenant-a").build();

    // when/then
    assertThatRejectsRequest(() -> client.activateJobs(request), "ActivateJobs");
  }

  @Test
  public void activateJobsResponseHasTenantId() {
    // given
    when(gateway.getIdentityMock().tenants().forToken(anyString()))
        .thenReturn(List.of(new Tenant("tenant-a", "A"), new Tenant("tenant-b", "B")));

    // when
    final Iterator<ActivateJobsResponse> responses =
        client.activateJobs(ActivateJobsRequest.newBuilder().build());
    assertThat(responses.hasNext()).isTrue();

    // then
    final ActivateJobsResponse response = responses.next();
    for (final ActivatedJob activatedJob : response.getJobsList()) {
      assertThat(activatedJob.getTenantId()).isEqualTo(TenantOwned.DEFAULT_TENANT_IDENTIFIER);
    }
  }
}
