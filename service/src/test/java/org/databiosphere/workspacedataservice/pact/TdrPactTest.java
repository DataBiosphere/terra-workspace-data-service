package org.databiosphere.workspacedataservice.pact;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTest;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import bio.terra.datarepo.model.SnapshotModel;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.datarepo.DataRepoClientFactory;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.datarepo.DataRepoException;
import org.databiosphere.workspacedataservice.datarepo.HttpDataRepoClientFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(PACT_TEST)
@PactConsumerTest
@PactTestFor(providerName = "datarepo", pactVersion = PactSpecVersion.V3)
class TdrPactTest {
  static final UUID dummySnapshotId = UUID.fromString("12345678-abc9-012d-3456-e7fab89cd01e");

  @Pact(consumer = "wds")
  public RequestResponsePact noSnapshotPact(PactDslWithProvider builder) {
    return builder
        .given("snapshot with given id doesn't exist", Map.of("id", dummySnapshotId.toString()))
        .uponReceiving("a snapshot request")
        .path("/api/repository/v1/snapshots/" + dummySnapshotId)
        .query("include=TABLES")
        .method("GET")
        .willRespondWith()
        .status(404)
        .toPact();
  }

  @Pact(consumer = "wds")
  public RequestResponsePact noAccessToSnapshotPact(PactDslWithProvider builder) {
    return builder
        .given(
            "user does not have access to snapshot with given id",
            Map.of("id", dummySnapshotId.toString()))
        .uponReceiving("a snapshot request")
        .path("/api/repository/v1/snapshots/" + dummySnapshotId)
        .matchQuery("include", "TABLES")
        .method("GET")
        .willRespondWith()
        .status(403)
        .toPact();
  }

  @Pact(consumer = "wds")
  public RequestResponsePact userHasAccessToSnapshotPact(PactDslWithProvider builder) {
    return builder
        .given(
            "user has access to snapshot with given id", Map.of("id", dummySnapshotId.toString()))
        .uponReceiving("a snapshot request")
        .path("/api/repository/v1/snapshots/" + dummySnapshotId)
        .query("include=TABLES")
        .method("GET")
        .willRespondWith()
        .status(200)
        .body(
            newJsonBody(
                    snapshot -> {
                      snapshot.stringValue("id", dummySnapshotId.toString());
                      snapshot.stringType("name");
                      snapshot.minArrayLike(
                          "tables",
                          /* minSize= */ 1,
                          table -> {
                            table.stringType("name");
                          });
                    })
                .build())
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "noSnapshotPact")
  void testNoSnapshot(MockServer mockServer) {
    DataRepoClientFactory clientFactory = new HttpDataRepoClientFactory(mockServer.getUrl());
    DataRepoDao dataRepoDao = new DataRepoDao(clientFactory);

    assertThrows(
        DataRepoException.class,
        () -> dataRepoDao.getSnapshot(dummySnapshotId),
        "nonexistent snapshot should return 404");
  }

  @Test
  @PactTestFor(pactMethod = "noAccessToSnapshotPact")
  void testNoAccessToSnapshot(MockServer mockServer) {
    DataRepoClientFactory clientFactory = new HttpDataRepoClientFactory(mockServer.getUrl());
    DataRepoDao dataRepoDao = new DataRepoDao(clientFactory);

    assertThrows(
        DataRepoException.class,
        () -> dataRepoDao.getSnapshot(dummySnapshotId),
        "nonexistent snapshot should return 403");
  }

  @Test
  @PactTestFor(pactMethod = "userHasAccessToSnapshotPact")
  void testUserHasAccessToSnapshot(MockServer mockServer) {
    DataRepoClientFactory clientFactory = new HttpDataRepoClientFactory(mockServer.getUrl());
    DataRepoDao dataRepoDao = new DataRepoDao(clientFactory);

    SnapshotModel snapshot = dataRepoDao.getSnapshot(dummySnapshotId);
    assertNotNull(snapshot, "Snapshot request should return a snapshot");
    assertNotNull(snapshot.getName(), "Snapshot response should have a name");
    assertEquals(dummySnapshotId, snapshot.getId(), "Snapshot id should match the requested id");

    var tables = snapshot.getTables();
    assertFalse(tables.isEmpty());
    tables.forEach(table -> assertNotNull(table.getName()));
  }
}
