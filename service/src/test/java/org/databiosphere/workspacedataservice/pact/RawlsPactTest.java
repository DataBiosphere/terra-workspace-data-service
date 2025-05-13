package org.databiosphere.workspacedataservice.pact;

import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.databiosphere.workspacedataservice.pact.TestHeaderSupport.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonArray;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.provider.spring.SpringRestPactRunner;
import com.google.common.collect.ImmutableMap;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.rawls.BearerAuthRequestInitializer;
import org.databiosphere.workspacedataservice.rawls.RawlsApi;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Tag(PACT_TEST)
@ExtendWith(PactConsumerTestExt.class)
@RunWith(SpringRestPactRunner.class)
class RawlsPactTest {

  private static final String WORKSPACE_UUID = "facade00-0000-4000-a000-000000000000";
  private static final String RESOURCE_UUID = "5ca1ab1e-0000-4000-a000-000000000000";

  @Pact(consumer = "cwds", provider = "rawls")
  RequestResponsePact createSnapshotPact(PactDslWithProvider builder) {

    var snapshotRequest = new PactDslJsonArray().stringValue(RESOURCE_UUID);
    return builder
        .given(
            "a workspace with the given {workspaceId} exists",
            ImmutableMap.of("workspaceId", WORKSPACE_UUID))
        .given("policies allowing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .pathFromProviderState(
            "/api/workspaces/${workspaceId}/snapshots/v3",
            String.format("/api/workspaces/%s/snapshots/v3", WORKSPACE_UUID))
        .method("POST")
        .headers(contentTypeJson())
        .body(snapshotRequest)
        .willRespondWith()
        .status(HttpStatus.CREATED.value())
        .headers(contentTypeJson())
        //        .body(newJsonBody(body -> {}).build())
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "createSnapshotPact", pactVersion = PactSpecVersion.V3)
  void testCreateSnapshot(MockServer mockServer) {
    RawlsClient rawlsClient = getRawlsClient(mockServer);
    assertDoesNotThrow(
        () ->
            rawlsClient.createSnapshotReferences(
                UUID.fromString(WORKSPACE_UUID), List.of(UUID.fromString(RESOURCE_UUID))));
  }

  private RawlsClient getRawlsClient(MockServer mockServer) {
    TestObservationRegistry observationRegistry = mock(TestObservationRegistry.class);
    when(observationRegistry.observationConfig())
        .thenReturn(new ObservationRegistry.ObservationConfig());
    RestClient restClient =
        RestClient.builder()
            .baseUrl(mockServer.getUrl())
            .observationRegistry(observationRegistry)
            .requestInitializer(new BearerAuthRequestInitializer())
            .build();
    HttpServiceProxyFactory httpServiceProxyFactory =
        HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();

    RawlsApi rawlsApi = httpServiceProxyFactory.createClient(RawlsApi.class);
    return new RawlsClient(rawlsApi, new RestClientRetry(observationRegistry));
  }
}
