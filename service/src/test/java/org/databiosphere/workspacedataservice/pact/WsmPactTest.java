package org.databiosphere.workspacedataservice.pact;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.databiosphere.workspacedataservice.pact.TestHeaderSupport.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

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
import com.google.common.collect.ImmutableMap;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.WsmSnapshotSupport;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.sam.BearerTokenFilter;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.HttpWorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Tag(PACT_TEST)
@PactConsumerTest
@PactTestFor(providerName = "workspacemanager", pactVersion = PactSpecVersion.V3)
class WsmPactTest {
  // copied from DslPart.UUID_REGEX, used to configure Pact to accept a wildcard UUID as the
  // workspaceId path param
  private static final String UUID_REGEX_PATTERN =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  // UUIDs are hardcoded to prevent churn in pactfiles, and intended to be human memorable
  private static final UUID WORKSPACE_UUID =
      UUID.fromString("facade00-0000-4000-a000-000000000000");
  private static final WorkspaceId WORKSPACE_ID = WorkspaceId.of(WORKSPACE_UUID);
  private static final UUID SNAPSHOT_UUID = UUID.fromString("decade00-0000-4000-a000-000000000000");
  private static final UUID RESOURCE_UUID = UUID.fromString("5ca1ab1e-0000-4000-a000-000000000000");
  private static final String SNAPSHOT_NAME = "hardcodedSnapshotName";
  private static final String USER_EMAIL = "fake.user@e.mail";
  private static final int NUM_SNAPSHOTS_THAT_EXIST = 3;
  private static final int NUM_SNAPSHOTS_REQUESTED = NUM_SNAPSHOTS_THAT_EXIST + 2;
  @MockitoBean ActivityLogger activityLogger;

  @BeforeEach
  void setUp() {
    // mock all requests to be authorized by the given bearer token
    var requestAttributes = new ServletRequestAttributes(new MockHttpServletRequest());
    requestAttributes.setAttribute(
        BearerTokenFilter.ATTRIBUTE_NAME_TOKEN, BEARER_TOKEN, SCOPE_REQUEST);
    RequestContextHolder.setRequestAttributes(requestAttributes);
  }

  @Pact(consumer = "wds")
  RequestResponsePact createDataRepoSnapshotReference_ok(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given("policies allowing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .method(HttpMethod.POST.name())
        .matchPath(snapshotPath(UUID_REGEX_PATTERN), snapshotPath(WORKSPACE_UUID.toString()))
        .headers(mergeHeaders(authorization(BEARER_TOKEN), contentTypeJson()))
        .body(createSnapshotReferenceBody(SNAPSHOT_NAME))
        .willRespondWith()
        .status(HttpStatus.OK.value())
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
  @PactTestFor(pactMethod = "createDataRepoSnapshotReference_ok")
  void linkSnapshotForPolicy_ok(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var snapshotModel = buildSnapshotModel();

    assertDoesNotThrow(
        () -> wsmDao.linkSnapshotForPolicy(WorkspaceId.of(WORKSPACE_UUID), snapshotModel));
  }

  @Pact(consumer = "wds")
  RequestResponsePact createDataRepoSnapshotReference_conflict(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given("policies preventing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .method(HttpMethod.POST.name())
        .matchPath(snapshotPath(UUID_REGEX_PATTERN), snapshotPath(WORKSPACE_UUID.toString()))
        .headers(mergeHeaders(authorization(BEARER_TOKEN), contentTypeJson()))
        .body(createSnapshotReferenceBody(SNAPSHOT_NAME))
        .willRespondWith()
        .status(HttpStatus.CONFLICT.value())
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body ->
                        // We're explicitly looking for a message about policies conflicting here
                        // because there are other potential reasons for a 409, and we want to be
                        // sure the provider is simulating the correct scenario.
                        body.stringMatcher(
                            "message",
                            "^(.*)(policy|policies)(.*)conflict(.*)$",
                            "Workspace policies conflict with source"))
                .build())
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "createDataRepoSnapshotReference_conflict")
  void linkSnapshotForPolicy_conflict(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var snapshotModel = buildSnapshotModel();

    WorkspaceId workspaceId = WorkspaceId.of(WORKSPACE_UUID);
    assertThrows(
        WorkspaceManagerException.class,
        () -> wsmDao.linkSnapshotForPolicy(workspaceId, snapshotModel));
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
                          p.stringValue("key", WorkspaceManagerDao.PROP_PURPOSE);
                          p.stringValue("value", WorkspaceManagerDao.PURPOSE_POLICY);
                        });
                  });
            })
        .build();
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact enumerateStorageContainers_noneExist(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure cloud context exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .given(
            "no Azure storage container resources exist for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .uponReceiving("a request to enumerate resources")
        .method(HttpMethod.GET.name())
        .matchPath(
            enumerateResourcesPath(UUID_REGEX_PATTERN),
            enumerateResourcesPath(WORKSPACE_UUID.toString()))
        .matchQuery(
            "resource",
            ResourceType.AZURE_STORAGE_CONTAINER.toString(),
            ResourceType.AZURE_STORAGE_CONTAINER.toString())
        .matchQuery("offset", /* regex= */ "[0-9]+", /* example= */ "0")
        .matchQuery("limit", /* regex= */ "[1-9](0-9)*", /* example= */ "1")
        .headers(mergeHeaders(authorization(BEARER_TOKEN), acceptJson()))
        .willRespondWith()
        .status(HttpStatus.OK.value())
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
  RequestResponsePact enumerateStorageContainers_oneExists(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure cloud context exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure storage container resource exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .uponReceiving("a request to enumerate storage containers")
        .method(HttpMethod.GET.name())
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
        .headers(mergeHeaders(authorization(BEARER_TOKEN), acceptJson()))
        .willRespondWith()
        .status(HttpStatus.OK.value())
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
  RequestResponsePact enumerateSnapshots_noneExist(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given(
            "no snapshot resources exist for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .uponReceiving("a request to enumerate snapshot resources")
        .method(HttpMethod.GET.name())
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
        .headers(mergeHeaders(authorization(BEARER_TOKEN), acceptJson()))
        .willRespondWith()
        .status(HttpStatus.OK.value())
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
  RequestResponsePact enumerateSnapshots_someExist(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given(
            "{num_snapshots} snapshots exist for the given {workspace_id}",
            new ImmutableMap.Builder<String, String>()
                .put("num_snapshots", String.valueOf(NUM_SNAPSHOTS_THAT_EXIST))
                .put("workspace_id", WORKSPACE_UUID.toString())
                .build())
        .uponReceiving("a paginated request to enumerate snapshot resources")
        .method(HttpMethod.GET.name())
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
        .headers(mergeHeaders(authorization(BEARER_TOKEN), acceptJson()))
        .willRespondWith()
        .status(HttpStatus.OK.value())
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
                                  metadata.stringValue(
                                      "cloningInstructions",
                                      CloningInstructionsEnum.REFERENCE.toString());
                                  // expect exactly one property, declaring the snapshot as being
                                  // for policy only
                                  metadata.minMaxArrayLike(
                                      "properties",
                                      /* minSize= */ 1,
                                      /* maxSize= */ 1,
                                      p -> {
                                        p.stringValue("key", WorkspaceManagerDao.PROP_PURPOSE);
                                        p.stringValue("value", WorkspaceManagerDao.PURPOSE_POLICY);
                                      });
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
  RequestResponsePact createAzureStorageContainerSasToken_ok(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure cloud context exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure storage container resource exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .given(
            "permission to create an azure storage container sas token for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .uponReceiving("a request to create an azure storage container sas token")
        .method(HttpMethod.POST.name())
        .matchPath(
            sasTokenPath(UUID_REGEX_PATTERN, UUID_REGEX_PATTERN),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .pathFromProviderState(
            sasTokenPath(WORKSPACE_UUID.toString(), "${storageContainerResourceId}"),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .headers(mergeHeaders(authorization(BEARER_TOKEN), contentTypeJson()))
        .willRespondWith()
        .status(HttpStatus.OK.value())
        .headers(contentTypeJson())
        .body(
            newJsonBody(
                    body -> {
                      // TODO: use matchUrl, just note it produces a very picky regex and is hard to
                      //   use, which is why it's not already done here
                      body.stringType("url");
                    })
                .build())
        .toPact();
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateStorageContainers_oneExists",
        "createAzureStorageContainerSasToken_ok"
      },
      pactVersion = PactSpecVersion.V3)
  void getBlobStorageUrl_ok(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);

    var blobStorageUrl = wsmDao.getBlobStorageUrl(WORKSPACE_ID, BEARER_TOKEN);

    assertNotNull(blobStorageUrl);
    // TODO: the sas URL has some pretty strict formatting requirements and making some assertions
    //   about what we can do with the URL when interacting with the BlobServiceClient might enhance
    //   our coverage
  }

  @Pact(consumer = "wds", provider = "workspacemanager")
  RequestResponsePact createAzureStorageContainerSasToken_forbidden(PactDslWithProvider builder) {
    return builder
        .given("authenticated with the given {email}", ImmutableMap.of("email", USER_EMAIL))
        .given(
            "a workspace with the given {id} exists",
            ImmutableMap.of("id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure cloud context exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .given(
            "an Azure storage container resource exists for the given {workspace_id}",
            ImmutableMap.of("workspace_id", WORKSPACE_UUID.toString()))
        .given("no permission to create an azure storage container sas token")
        .uponReceiving("a request to create an azure storage container sas token")
        .method(HttpMethod.POST.name())
        .matchPath(
            sasTokenPath(UUID_REGEX_PATTERN, UUID_REGEX_PATTERN),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .pathFromProviderState(
            sasTokenPath(WORKSPACE_UUID.toString(), "${storageContainerResourceId}"),
            sasTokenPath(WORKSPACE_UUID.toString(), RESOURCE_UUID.toString()))
        .headers(mergeHeaders(authorization(BEARER_TOKEN), contentTypeJson()))
        .willRespondWith()
        .status(HttpStatus.FORBIDDEN.value())
        .toPact();
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethods = {
        "enumerateStorageContainers_oneExists",
        "createAzureStorageContainerSasToken_forbidden"
      },
      pactVersion = PactSpecVersion.V3)
  void getBlobStorageUrl_forbidden(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var thrown =
        assertThrows(
            WorkspaceManagerException.class,
            () -> wsmDao.getBlobStorageUrl(WORKSPACE_ID, BEARER_TOKEN));
    assertEquals(HttpStatus.FORBIDDEN, thrown.getStatusCode());
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethod = "enumerateStorageContainers_noneExist",
      pactVersion = PactSpecVersion.V3)
  void getBlobStorageUrl_noStorageContainers(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var thrown =
        assertThrows(
            WorkspaceManagerException.class,
            () -> wsmDao.getBlobStorageUrl(WORKSPACE_ID, BEARER_TOKEN));
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, thrown.getStatusCode());
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethod = "enumerateSnapshots_noneExist",
      pactVersion = PactSpecVersion.V3)
  void existingPolicySnapshotIds_noneExist(MockServer mockServer) {
    var snapshotIds =
        wsmSnapshotSupport(mockServer).existingPolicySnapshotIds(NUM_SNAPSHOTS_REQUESTED);
    assertTrue(snapshotIds.isEmpty());
  }

  @Test
  @PactTestFor(
      providerName = "workspacemanager",
      pactMethod = "enumerateSnapshots_someExist",
      pactVersion = PactSpecVersion.V3)
  void existingPolicySnapshotIds_someExist(MockServer mockServer) {
    var snapshotIds =
        wsmSnapshotSupport(mockServer).existingPolicySnapshotIds(NUM_SNAPSHOTS_REQUESTED);
    assertEquals(NUM_SNAPSHOTS_THAT_EXIST, snapshotIds.size());
  }

  private WsmSnapshotSupport wsmSnapshotSupport(MockServer mockServer) {
    return new WsmSnapshotSupport(
        WorkspaceId.of(WORKSPACE_UUID), buildWsmDao(mockServer), activityLogger);
  }

  private static WorkspaceManagerDao buildWsmDao(MockServer mockServer) {
    WorkspaceManagerClientFactory clientFactory =
        new HttpWorkspaceManagerClientFactory(mockServer.getUrl());
    return new WorkspaceManagerDao(
        clientFactory, new RestClientRetry(TestObservationRegistry.create()));
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
}
