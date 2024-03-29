package org.databiosphere.workspacedataservice.pact;

import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.provider.spring.SpringRestPactRunner;
import io.micrometer.observation.ObservationRegistry;
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
import org.mockito.Mockito;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Tag(PACT_TEST)
@ExtendWith(PactConsumerTestExt.class)
@RunWith(SpringRestPactRunner.class)
@Import(TestObservationRegistryConfig.class)
public class RawlsPactTest {

  private static final UUID WORKSPACE_UUID =
      UUID.fromString("facade00-0000-4000-a000-000000000000");
  private static final UUID RESOURCE_UUID = UUID.fromString("5ca1ab1e-0000-4000-a000-000000000000");

  @Pact(consumer = "wds", provider = "rawls")
  public RequestResponsePact enumerateSnapshotsPact(PactDslWithProvider builder) {
    var snapshotListResponse =
        new PactDslJsonBody()
            .array("gcpDataRepoSnapshots")
            .object()
            .object("metadata")
            .uuid(
                "workspaceId",
                WORKSPACE_UUID) // TODO which of the fields in metadata do we actually
            // care about
            .uuid("resourceId", RESOURCE_UUID)
            //            .stringType("name")
            //            .stringType("description")
            //            .stringType("resourceType")
            //            .stringType("stewardshipType")
            //            .stringType("cloudPlatform")
            //            .stringType("cloningInstructions")
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
        //        .path(String.format("/api/workspaces/%s/snapshots/v2", WORKSPACE_UUID))
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
        rawlsClient.enumerateDataRepoSnapshotReferences(WORKSPACE_UUID, 0, 10);
    assertNotNull(snapshots);
    assertEquals(1, snapshots.gcpDataRepoSnapshots().size());
  }

  private RawlsClient getRawlsClient(MockServer mockServer) {
    // TODO copied from RawlsClientConfig; can I autowire instead?
    ObservationRegistry observationRegistry = Mockito.mock(ObservationRegistry.class);
    when(observationRegistry.observationConfig())
        .thenReturn(new ObservationRegistry.ObservationConfig());

    RestClient restClient =
        RestClient.builder()
            //            .observationRegistry(observationRegistry)
            .baseUrl(mockServer.getUrl())
            .requestInitializer(new BearerAuthRequestInitializer())
            .build();
    HttpServiceProxyFactory httpServiceProxyFactory =
        HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();

    RawlsApi rawlsApi = httpServiceProxyFactory.createClient(RawlsApi.class);
    return new RawlsClient(rawlsApi, new RestClientRetry());
  }
}
