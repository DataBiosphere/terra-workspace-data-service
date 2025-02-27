package org.databiosphere.workspacedataservice.pact;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static bio.terra.workspace.model.CloningInstructionsEnum.*;
import static bio.terra.workspace.model.CloningInstructionsEnum.NOTHING;
import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.databiosphere.workspacedataservice.pact.TestHeaderSupport.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.DslPart;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
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
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupport;
import org.databiosphere.workspacedataservice.rawls.BearerAuthRequestInitializer;
import org.databiosphere.workspacedataservice.rawls.RawlsApi;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
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
  RequestResponsePact enumerateSnapshotsPact(PactDslWithProvider builder) {
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
        .body(getListSnapshotsBody())
        .toPact();
  }

  private static DslPart getListSnapshotsBody() {
    // This response needs to be turned into a ResourceList, so the important thing is that it has:
    // - a list of DataRepoSnapshotResources called "gcpDataRepoSnapshots"
    // - each of which has an "attributes" object and a "metadata" object
    return newJsonBody(
            body ->
                body.minArrayLike(
                    "gcpDataRepoSnapshots",
                    /* minSize= */ 1,
                    snapshots ->
                        snapshots
                            .object(
                                "metadata",
                                metadata ->
                                    metadata
                                        .uuid("workspaceId", UUID.fromString(WORKSPACE_UUID))
                                        .uuid("resourceId")
                                        .stringType("name")
                                        .stringType("description")
                                        .stringType("resourceType")
                                        .stringType("stewardshipType")
                                        .stringValue("cloningInstructions", NOTHING.toString())
                                        .array("properties", emptyArray -> {}))
                            .object(
                                "attributes",
                                attributes ->
                                    attributes.stringType("instanceName").stringType("snapshot"))))
        .build();
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

  private static DslPart getSnapshotRequestBody() {
    return newJsonBody(
            body ->
                body.stringValue("snapshotId", RESOURCE_UUID)
                    .stringType("name")
                    .stringType("description")
                    .stringValue("cloningInstructions", REFERENCE.toString())
                    .object(
                        "properties",
                        properties ->
                            properties.stringValue(
                                SnapshotSupport.PROP_PURPOSE, SnapshotSupport.PURPOSE_POLICY)))
        .build();
  }

  @Pact(consumer = "cwds", provider = "rawls")
  RequestResponsePact createSnapshotPact(PactDslWithProvider builder) {

    var snapshotRequest =
        new PactDslJsonBody()
            .stringValue("snapshotId", RESOURCE_UUID)
            .stringType("name")
            .stringType("description")
            .stringValue("cloningInstructions", REFERENCE.toString())
            .object("properties")
            .stringValue(SnapshotSupport.PROP_PURPOSE, SnapshotSupport.PURPOSE_POLICY)
            .closeObject();
    return builder
        .given(
            "a workspace with the given {workspaceId} exists",
            ImmutableMap.of("workspaceId", WORKSPACE_UUID))
        .given("policies allowing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .pathFromProviderState(
            "/api/workspaces/${workspaceId}/snapshots/v2",
            String.format("/api/workspaces/%s/snapshots/v2", WORKSPACE_UUID))
        .method("POST")
        .headers(contentTypeJson())
        .body(getSnapshotRequestBody())
        .willRespondWith()
        .status(HttpStatus.CREATED.value())
        .headers(contentTypeJson())
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
