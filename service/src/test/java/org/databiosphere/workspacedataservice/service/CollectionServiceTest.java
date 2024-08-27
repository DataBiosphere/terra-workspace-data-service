package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("control-plane")
@DirtiesContext
@SpringBootTest(
    properties = { // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // aggressive retry settings so unit tests don't run too long
      "rest.retry.maxAttempts=2",
      "rest.retry.backoff.delay=3",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
class CollectionServiceTest {

  @Autowired private CollectionService collectionService;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TwdsProperties twdsProperties;

  @BeforeEach
  @AfterEach
  void dropCollectionSchemas() {
    // Delete all collections
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  @Test
  void testCreateDefaultIsIdempotent() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // at the start of the test, we expect the default collection does not exist
    assertThat(collectionService.find(workspaceId, collectionId)).isEmpty();
    assertThat(collectionService.list(workspaceId)).isEmpty();

    // issue the call to create the default collection a few times; this call should be idempotent
    for (int i = 0; i < 5; i++) {
      collectionService.createDefaultCollection(workspaceId);
      assertThat(collectionService.find(workspaceId, collectionId)).isPresent();
      assertThat(collectionService.list(workspaceId)).hasSize(1);
    }
  }

  @Test
  void testFindAndCreateDefault() {

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // find should be empty to start
    assertThat(collectionService.find(workspaceId, collectionId)).isEmpty();

    // create default collection
    collectionService.createDefaultCollection(workspaceId);

    // find should be present after we create the collection
    var found = collectionService.find(workspaceId, collectionId);
    assertThat(found).isPresent();
    assertEquals(collectionId.id(), found.get().getId());

    // delete collection once more
    collectionService.delete(workspaceId, collectionId);

    // find should be empty again
    assertThat(collectionService.find(workspaceId, collectionId)).isEmpty();
  }
}
