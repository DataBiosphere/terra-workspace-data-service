package org.databiosphere.workspacedataservice.pact;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.workspacemanager.HttpWorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("pact-test")
@PactConsumerTest
@PactTestFor(providerName = "workspacemanager", pactVersion = PactSpecVersion.V3)
public class WsmPactTest {
  // copied from DslPart.UUID_REGEX, used to configure Pact to accept a wildcard UUID as the
  // workspaceId path param
  private static final String UUID_REGEX_PATTERN =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  // the two UUIDs are hardcoded to prevent churn in pactfiles, and intended to be human memorable
  private static final UUID WORKSPACE_UUID =
      UUID.fromString("facade00-0000-4000-a000-000000000000");
  private static final UUID SNAPSHOT_UUID = UUID.fromString("decade00-0000-4000-a000-000000000000");
  private static final String SNAPSHOT_NAME = "hardcodedSnapshotName";
  private static final String SNAPSHOT_CREATOR_EMAIL = "snapshot.creator@e.mail";

  private String snapshotPath(String workspaceIdPart) {
    return String.format(
        "/api/workspaces/v1/%s/resources/referenced/datarepo/snapshots", workspaceIdPart);
  }

  @Pact(consumer = "wds")
  RequestResponsePact linkSnapshotForPolicySuccess(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given("authenticated with the given email", Map.of("email", SNAPSHOT_CREATOR_EMAIL))
        .given("policies allowing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .matchPath(snapshotPath(UUID_REGEX_PATTERN), snapshotPath(WORKSPACE_UUID.toString()))
        .method("POST")
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

  @Pact(consumer = "wds")
  RequestResponsePact linkSnapshotForPolicyConflict(PactDslWithProvider builder) {
    return builder
        .given("a workspace with the given id exists", Map.of("id", WORKSPACE_UUID.toString()))
        .given("authenticated with the given email", Map.of("email", SNAPSHOT_CREATOR_EMAIL))
        .given("policies preventing snapshot reference creation")
        .uponReceiving("a request to create a snapshot reference")
        .matchPath(snapshotPath(UUID_REGEX_PATTERN), snapshotPath(WORKSPACE_UUID.toString()))
        .method("POST")
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
  @PactTestFor(pactMethod = "linkSnapshotForPolicySuccess")
  void testLinkSnapshotForPolicySuccess(MockServer mockServer) {
    var wsmDao = buildWsmDao(mockServer);
    var snapshotModel = buildSnapshotModel();

    assertDoesNotThrow(() -> wsmDao.linkSnapshotForPolicy(snapshotModel));
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

  private WorkspaceManagerDao buildWsmDao(MockServer mockServer) {
    WorkspaceManagerClientFactory clientFactory =
        new HttpWorkspaceManagerClientFactory(mockServer.getUrl());
    return new WorkspaceManagerDao(clientFactory, WORKSPACE_UUID.toString(), new RestClientRetry());
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
                    snapshot.uuid("snapshot");
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

  private Map<String, String> contentTypeJson() {
    Map<String, String> headers = new HashMap<>();
    // pact will automatically assume an expected Content-Type of "application/json; charset=UTF-8"
    // unless we explicitly tell it otherwise
    headers.put("Content-Type", "application/json");
    return headers;
  }
}
