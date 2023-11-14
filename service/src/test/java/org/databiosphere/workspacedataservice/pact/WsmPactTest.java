package org.databiosphere.workspacedataservice.pact;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.DslPart;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTest;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.CloningInstructionsEnum;
import bio.terra.workspace.model.ResourceType;
import bio.terra.workspace.model.StewardshipType;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.TdrSnapshotSupport;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.workspacemanager.HttpWorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

@Tag("pact-test")
@PactConsumerTest
@PactTestFor(providerName = "workspacemanager", pactVersion = PactSpecVersion.V3)
public class WsmPactTest {
  public static final int NUM_SNAPSHOTS_THAT_EXIST = 3;
  public static final int NUM_SNAPSHOTS_REQUESTED = NUM_SNAPSHOTS_THAT_EXIST + 2;
  // copied from DslPart.UUID_REGEX, used to configure Pact to accept a wildcard UUID as the
  // workspaceId path param
  private static final String UUID_REGEX_PATTERN =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  // UUIDs are hardcoded to prevent churn in pactfiles, and intended to be human memorable
  private static final UUID WORKSPACE_UUID =
      UUID.fromString("facade00-0000-4000-a000-000000000000");
  private static final UUID SNAPSHOT_UUID = UUID.fromString("decade00-0000-4000-a000-000000000000");
  private static final UUID RESOURCE_UUID = UUID.fromString("5ca1ab1e-0000-4000-a000-000000000000");
  private static final String SNAPSHOT_NAME = "hardcodedSnapshotName";
  private static final String SNAPSHOT_CREATOR_EMAIL = "snapshot.creator@e.mail";

  @Pact(consumer = "wds")
  RequestResponsePact linkSnapshotForPolicySuccess(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given("authenticated with the given email", Map.of("email", SNAPSHOT_CREATOR_EMAIL))
        .given("policies allowing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .method("POST")
        .matchPath(snapshotPath(UUID_REGEX_PATTERN), snapshotPath(WORKSPACE_UUID.toString()))
        .headers(contentTypeJson())
        .body(createSnapshotReferenceBody(SNAPSHOT_NAME))
        .willRespondWith()
        .status(200) // ok
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      // put expectations here if we ever start reading fields in the code under
                      // test
                    })
                .build())
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "linkSnapshotForPolicySuccess")
  void testLinkSnapshotForPolicySuccess(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var snapshotModel = buildSnapshotModel();

    assertDoesNotThrow(() -> wsmDao.linkSnapshotForPolicy(snapshotModel));
  }

  @Pact(consumer = "wds")
  RequestResponsePact linkSnapshotForPolicyConflict(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given("authenticated with the given email", Map.of("email", SNAPSHOT_CREATOR_EMAIL))
        .given("policies preventing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .method("POST")
        .matchPath(snapshotPath(UUID_REGEX_PATTERN), snapshotPath(WORKSPACE_UUID.toString()))
        .headers(contentTypeJson())
        .body(createSnapshotReferenceBody(SNAPSHOT_NAME))
        .willRespondWith()
        .status(409) // conflict
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body ->
                        body.stringMatcher(
                            "message",
                            "^(.*)(policy|policies)(.*)conflict(.*)$",
                            "Workspace policies conflict with source"))
                .build())
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "linkSnapshotForPolicyConflict")
  void testLinkSnapshotForPolicyConflict(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var snapshotModel = buildSnapshotModel();

    assertThrows(
        WorkspaceManagerException.class, () -> wsmDao.linkSnapshotForPolicy(snapshotModel));
  }

  private SnapshotModel buildSnapshotModel() {
    return new SnapshotModel().id(SNAPSHOT_UUID).name(SNAPSHOT_NAME);
  }

  private DslPart createSnapshotReferenceBody(String snapshotName) {
    // metadata.name is a composite of <snapshotName>_<timestamp>
    String nameFormatString = String.format("'%s'_yyyyMMddHHmmss", snapshotName);

    return newJsonBody(
            body -> {
              body.object(
                  "snapshot",
                  snapshot -> {
                    snapshot.stringValue("instanceName", "terra");
                    snapshot.uuid("snapshot", SNAPSHOT_UUID);
                  });
              body.object(
                  "metadata",
                  metadata -> {
                    metadata.stringValue(
                        "cloningInstructions", CloningInstructionsEnum.REFERENCE.toString());
                    metadata.datetime("name", nameFormatString);
                    // expect exactly one property, declaring the snapshot as being for policy only
                    metadata.minMaxArrayLike(
                        "properties",
                        /* minSize= */ 1,
                        /* maxSize= */ 1,
                        p -> {
                          p.stringValue("key", "purpose");
                          p.stringValue("value", "policy");
                        });
                  });
            })
        .build();
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact enumerateStorageContainersWhenNoneExist(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given("no storage container resources exist for the given workspace_id")
        .uponReceiving("a request to enumerate resources")
        .method("GET")
        .matchPath(
            enumerateResourcesPath(UUID_REGEX_PATTERN),
            enumerateResourcesPath(WORKSPACE_UUID.toString()))
        .matchQuery(
            "resource",
            ResourceType.AZURE_STORAGE_CONTAINER.toString(),
            ResourceType.AZURE_STORAGE_CONTAINER.toString())
        .matchQuery("offset", /* regex= */ "[0-9]+", /* example= */ "0")
        .matchQuery("limit", /* regex= */ "[0-9]+", /* example= */ "0")
        .headers(acceptJson())
        .willRespondWith()
        .status(200)
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      body.array("resources", emptyArray -> {});
                    })
                .build())
        .toPact();
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact enumerateStorageContainersWhenOneExists(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given(
            "a storage container resource exists for the given workspace_id",
            Map.of("workspace_id", WORKSPACE_UUID.toString()))
        .uponReceiving("a request to enumerate storage containers")
        .method("GET")
        .matchPath(
            enumerateResourcesPath(UUID_REGEX_PATTERN),
            enumerateResourcesPath(WORKSPACE_UUID.toString()))
        .matchQuery(
            "resource",
            ResourceType.AZURE_STORAGE_CONTAINER.toString(),
            ResourceType.AZURE_STORAGE_CONTAINER.toString())
        .matchQuery("offset", "0", "0")
        .matchQuery(
            "limit",
            String.valueOf(NUM_SNAPSHOTS_REQUESTED),
            String.valueOf(NUM_SNAPSHOTS_REQUESTED))
        .headers(acceptJson())
        .willRespondWith()
        .status(200)
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      body.arrayContaining(
                          "resources",
                          resources -> {
                            // at least one resource should have a name matching the workspaceId
                            resources.object(
                                o -> {
                                  o.object(
                                      "metadata",
                                      m -> {
                                        m.uuid("workspaceId", WORKSPACE_UUID);
                                        m.valueFromProviderState(
                                            "resourceId",
                                            "storageContainerResourceId",
                                            RESOURCE_UUID.toString());
                                        m.uuid("resourceId");
                                        m.stringMatcher(
                                            "name",
                                            String.format("sc-%s", UUID_REGEX_PATTERN),
                                            String.format("sc-%s", WORKSPACE_UUID));
                                      });
                                });
                          });
                    })
                .build())
        .toPact();
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact enumerateSnapshotsWhenNoneExist(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given(
            "no snapshot resources exist for the given workspace_id",
            Map.of("workspace_id", WORKSPACE_UUID.toString()))
        .uponReceiving("a request to enumerate snapshot resources")
        .method("GET")
        .matchPath(
            enumerateResourcesPath(UUID_REGEX_PATTERN),
            enumerateResourcesPath(WORKSPACE_UUID.toString()))
        .matchQuery(
            "resource",
            ResourceType.DATA_REPO_SNAPSHOT.toString(),
            ResourceType.DATA_REPO_SNAPSHOT.toString())
        .matchQuery(
            "stewardship",
            StewardshipType.REFERENCED.toString(),
            StewardshipType.REFERENCED.toString())
        .matchQuery("offset", "0", "0")
        .matchQuery(
            "limit",
            String.valueOf(NUM_SNAPSHOTS_REQUESTED),
            String.valueOf(NUM_SNAPSHOTS_REQUESTED))
        .headers(acceptJson())
        .willRespondWith()
        .status(200)
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      body.array("resources", emptyArray -> {});
                    })
                .build())
        .toPact();
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact enumerateSnapshotsWhenSomeExist(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given(
            "{num_snapshots} snapshots exist for the given workspace_id",
            Map.of(
                "workspace_id",
                WORKSPACE_UUID.toString(),
                "num_snapshots",
                NUM_SNAPSHOTS_THAT_EXIST))
        .uponReceiving("a paginated request to enumerate snapshot resources")
        .method("GET")
        .matchPath(
            enumerateResourcesPath(UUID_REGEX_PATTERN),
            enumerateResourcesPath(WORKSPACE_UUID.toString()))
        .matchQuery(
            "resource",
            ResourceType.DATA_REPO_SNAPSHOT.toString(),
            ResourceType.DATA_REPO_SNAPSHOT.toString())
        .matchQuery(
            "stewardship",
            StewardshipType.REFERENCED.toString(),
            StewardshipType.REFERENCED.toString())
        .matchQuery("offset", "0", "0")
        .matchQuery(
            "limit",
            String.valueOf(NUM_SNAPSHOTS_REQUESTED),
            String.valueOf(NUM_SNAPSHOTS_REQUESTED))
        .headers(acceptJson())
        .willRespondWith()
        .status(200)
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      body.minMaxArrayLike(
                          "resources",
                          /* minSize= */ NUM_SNAPSHOTS_THAT_EXIST,
                          /* maxSize= */ NUM_SNAPSHOTS_THAT_EXIST,
                          resource -> {
                            resource.object(
                                "metadata",
                                metadata -> {
                                  metadata.uuid("resourceId");
                                });
                            resource.object(
                                "resourceAttributes",
                                attributes -> {
                                  attributes.object(
                                      "gcpDataRepoSnapshot",
                                      tdrSnapshot -> tdrSnapshot.uuid("snapshot"));
                                });
                          });
                    })
                .build())
        .toPact();
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact createAzureStorageContainerSasTokenSuccess(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given(
            "a storage container resource exists for the given workspace_id",
            Map.of("workspace_id", WORKSPACE_UUID.toString()))
        .given("permission to create an azure storage container sas token")
        .uponReceiving("a request to create an azure storage container sas token")
        .method("POST")
        .matchPath(
            sasTokenPath(UUID_REGEX_PATTERN, UUID_REGEX_PATTERN),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .pathFromProviderState(
            sasTokenPath(WORKSPACE_UUID.toString(), "${storageContainerResourceId}"),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .headers(contentTypeJson())
        .willRespondWith()
        .status(200) // success
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      body.matchUrl("url", /* basePath= */ ".*", /* pathFragments= */ ".*");
                    })
                .build())
        .toPact();
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateStorageContainersWhenOneExists",
        "createAzureStorageContainerSasTokenSuccess"
      },
      pactVersion = PactSpecVersion.V3)
  void testGetBlobStorageUrlSuccess(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);

    var actualStorageUrl =
        wsmDao.getBlobStorageUrl(WORKSPACE_UUID.toString(), /* authToken= */ null);

    assertNotNull(actualStorageUrl);
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact createAzureStorageContainerSasTokenForbidden(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given(
            "a storage container resource exists for the given workspace_id",
            Map.of("workspace_id", WORKSPACE_UUID.toString()))
        .given("no permission to create an azure storage container sas token")
        .uponReceiving("a request to create an azure storage container sas token")
        .method("POST")
        .matchPath(
            sasTokenPath(UUID_REGEX_PATTERN, UUID_REGEX_PATTERN),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .pathFromProviderState(
            sasTokenPath(WORKSPACE_UUID.toString(), "${storageContainerResourceId}"),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .headers(contentTypeJson())
        .willRespondWith()
        .status(403)
        .toPact();
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateStorageContainersWhenOneExists",
        "createAzureStorageContainerSasTokenForbidden"
      },
      pactVersion = PactSpecVersion.V3)
  void testGetBlobStorageUrlNoPermission(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var thrown =
        assertThrows(
            WorkspaceManagerException.class,
            () -> wsmDao.getBlobStorageUrl(WORKSPACE_UUID.toString(), /* authToken= */ null));
    assertEquals(HttpStatus.FORBIDDEN, thrown.getStatus());
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateStorageContainersWhenNoneExist",
      },
      pactVersion = PactSpecVersion.V3)
  void testGetBlobStorageUrlNoResources(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var thrown =
        assertThrows(
            WorkspaceManagerException.class,
            () -> wsmDao.getBlobStorageUrl(WORKSPACE_UUID.toString(), /* authToken= */ null));
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, thrown.getStatus());
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateSnapshotsWhenNoneExist",
      },
      pactVersion = PactSpecVersion.V3)
  void testExistingPolicySnapshotIdsEmpty(MockServer mockServer) {
    var snapshotIds =
        tdrSnapshotSupport(mockServer).existingPolicySnapshotIds(NUM_SNAPSHOTS_REQUESTED);
    assertTrue(snapshotIds.isEmpty());
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateSnapshotsWhenSomeExist",
      },
      pactVersion = PactSpecVersion.V3)
  void testExistingPolicySnapshotIdsWithSnapshotsPresent(MockServer mockServer) {
    var snapshotIds =
        tdrSnapshotSupport(mockServer).existingPolicySnapshotIds(NUM_SNAPSHOTS_REQUESTED);
    assertEquals(NUM_SNAPSHOTS_THAT_EXIST, snapshotIds.size());
  }

  private static TdrSnapshotSupport tdrSnapshotSupport(MockServer mockServer) {
    return new TdrSnapshotSupport(WORKSPACE_UUID, buildWsmDao(mockServer), new RestClientRetry());
  }

  private static WorkspaceManagerDao buildWsmDao(MockServer mockServer) {
    WorkspaceManagerClientFactory clientFactory =
        new HttpWorkspaceManagerClientFactory(mockServer.getUrl());
    return new WorkspaceManagerDao(clientFactory, WORKSPACE_UUID.toString(), new RestClientRetry());
  }

  // paths
  private static String snapshotPath(String workspaceIdPart) {
    return String.format(
        "/api/workspaces/v1/%s/resources/referenced/datarepo/snapshots", workspaceIdPart);
  }

  private static String enumerateResourcesPath(String workspaceIdPart) {
    return String.format("/api/workspaces/v1/%s/resources", workspaceIdPart);
  }

  private static String sasTokenPath(String workspaceIdPart, String resourceIdPart) {
    return String.format(
        "/api/workspaces/v1/%s/resources/controlled/azure/storageContainer/%s/getSasToken",
        workspaceIdPart, resourceIdPart);
  }

  // headers
  private static Map<String, String> contentTypeJson() {
    Map<String, String> headers = new HashMap<>();
    // pact will automatically assume an expected Content-Type of "application/json; charset=UTF-8"
    // unless we explicitly tell it otherwise
    headers.put("Content-Type", "application/json");
    return headers;
  }

  private static Map<String, String> acceptJson() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    return headers;
  }
}
