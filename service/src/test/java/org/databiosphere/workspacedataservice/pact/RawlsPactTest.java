package org.databiosphere.workspacedataservice.pact;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.provider.spring.SpringRestPactRunner;
import bio.terra.workspace.model.CloningInstructionsEnum;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.observability.TestObservationRegistryConfig;
import org.databiosphere.workspacedataservice.rawls.BearerAuthRequestInitializer;
import org.databiosphere.workspacedataservice.rawls.RawlsApi;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Tag(PACT_TEST)
@ExtendWith(PactConsumerTestExt.class)
@RunWith(SpringRestPactRunner.class)
@Import(TestObservationRegistryConfig.class)
public class RawlsPactTest {

  private static final String WORKSPACE_UUID = "facade00-0000-4000-a000-000000000000";
  private static final String RESOURCE_UUID = "5ca1ab1e-0000-4000-a000-000000000000";

  @Pact(consumer = "wds", provider = "rawls")
  public RequestResponsePact enumerateSnapshotsPact(PactDslWithProvider builder) {
    var snapshotListResponse =
        new PactDslJsonBody()
            .array("gcpDataRepoSnapshots")
            .object()
            .object("metadata")
            .stringValue(
                "workspaceId",
                WORKSPACE_UUID) // TODO which of the fields in metadata do we actually
            // care about
            .stringValue("resourceId", RESOURCE_UUID)
            //            .stringValue("name", "testName")
            //            .stringValue("description", "testDescription")
            .stringType("name")
            .stringType("description")
            .stringType("resourceType")
            .stringType("stewardshipType")
            .stringValue("cloningInstructions", CloningInstructionsEnum.NOTHING.toString())
            .array("properties")
            .closeArray() // TODO what do we want in properties
            .closeObject()
            .object("attributes")
            .stringType("instanceName")
            .stringType("snapshot")
            .closeObject()
            .closeObject()
            .closeArray();
    return builder
        .given("one snapshot in the given workspace", Map.of("workspaceId", WORKSPACE_UUID))
        .uponReceiving("a request for the workspace's snapshots")
        .pathFromProviderState(
            "/api/workspaces/${workspaceId}/snapshots/v2",
            String.format("/api/workspaces/%s/snapshots/v2", WORKSPACE_UUID))
        .matchQuery("offset", "0")
        .matchQuery("limit", "10")
        .method("GET")
        .willRespondWith()
        .status(200)
        .body(snapshotListResponse)
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "enumerateSnapshotsPact", pactVersion = PactSpecVersion.V3)
  void testRawlsEnumerateSnapshots(MockServer mockServer) {
    RawlsClient rawlsClient = getRawlsClient(mockServer);
    SnapshotListResponse snapshots =
        rawlsClient.enumerateDataRepoSnapshotReferences(UUID.fromString(WORKSPACE_UUID), 0, 10);
    assertNotNull(snapshots);
    assertEquals(1, snapshots.gcpDataRepoSnapshots().size());
  }

  @Pact(consumer = "wds", provider = "rawls")
  public RequestResponsePact createSnapshotPact(PactDslWithProvider builder) {
    return builder
        .given("policies allowing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .pathFromProviderState(
            "/api/workspaces/${workspaceId}/snapshots/v2",
            String.format("/api/workspaces/%s/snapshots/v2", WORKSPACE_UUID))
        .method("POST")
        .headers(PactTestSupport.contentTypeJson())
        .body(
            new PactDslJsonBody()
                .stringValue("snapshotId", RESOURCE_UUID)
                .stringType("name")
                .stringType("description")
                .stringValue("cloningInstructions", CloningInstructionsEnum.REFERENCE.toString())
                .object("properties")
                .stringValue("purpose", "policy")
                .closeObject())
        .willRespondWith()
        .status(HttpStatus.OK.value())
        .headers(PactTestSupport.contentTypeJson())
        .body(newJsonBody(body -> {}).build())
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "createSnapshotPact", pactVersion = PactSpecVersion.V3)
  void testCreateSnapshot(MockServer mockServer) {
    RawlsClient rawlsClient = getRawlsClient(mockServer);
    assertDoesNotThrow(
        () ->
            rawlsClient.createSnapshotReference(
                UUID.fromString(WORKSPACE_UUID), UUID.fromString(RESOURCE_UUID)));
  }

  private RawlsClient getRawlsClient(MockServer mockServer) {
    RestClient restClient =
        RestClient.builder()
            .baseUrl(mockServer.getUrl())
            .requestInitializer(new BearerAuthRequestInitializer())
            .build();
    HttpServiceProxyFactory httpServiceProxyFactory =
        HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();

    RawlsApi rawlsApi = httpServiceProxyFactory.createClient(RawlsApi.class);
    return new RawlsClient(rawlsApi, new RestClientRetry());
  }
}
