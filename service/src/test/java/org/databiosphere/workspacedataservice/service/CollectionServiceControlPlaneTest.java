package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"control-plane"})
@DirtiesContext
@SpringBootTest(
    properties = {
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/",
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
    })
class CollectionServiceControlPlaneTest {

  @Autowired private CollectionService collectionService;
  @Autowired private WorkspaceRepository workspaceRepository;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;

  @MockBean RawlsClient rawlsClient;

  @BeforeEach
  @AfterEach
  void cleanDatabase() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  /**
   * workspace is powered by entity service; collection is virtual and will always return its own id
   * as the workspace id
   */
  @Test
  void getWorkspaceIdEntityService() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // save this workspace as RAWLS
    WorkspaceRecord workspaceRecord =
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.RAWLS, /* newFlag= */ true);
    workspaceRepository.save(workspaceRecord);

    // note: we do not create any collections here

    WorkspaceId actual = collectionService.getWorkspaceId(collectionId);

    assertEquals(workspaceId, actual);
  }

  /** workspace is powered by WDS and collection exists */
  @Test
  void getWorkspaceIdWdsExists() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // save this workspace as WDS
    WorkspaceRecord workspaceRecord =
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true);
    workspaceRepository.save(workspaceRecord);

    // save a collection
    CollectionRequestServerModel collection =
        new CollectionRequestServerModel("name", "description");
    CollectionId collectionId =
        CollectionId.of(collectionService.save(workspaceId, collection).getId());

    WorkspaceId actual = collectionService.getWorkspaceId(collectionId);

    assertEquals(workspaceId, actual);
  }

  /** workspace is powered by WDS; looking for the default collection; collection does not exist */
  @Test
  void getWorkspaceIdWdsNonexistentDefault() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // save this workspace as WDS
    WorkspaceRecord workspaceRecord =
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true);
    workspaceRepository.save(workspaceRecord);

    // note: we do not create any collections here

    // getWorkspaceId() should throw an exception for the default collection
    MissingObjectException actual =
        assertThrows(
            MissingObjectException.class,
            () -> collectionService.getWorkspaceId(collectionId),
            "for the default collection id");
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  /** looking for a random collection; collection does not exist */
  @Test
  void getWorkspaceIdNonexistentRandom() {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // note: we do not create any collections here

    // since the random collection wasn't found, we couldn't determine its workspace. This will
    // generate a request to Rawls to see if Rawls knows about a workspace whose id is the same
    // as the collection
    when(rawlsClient.getWorkspaceDetails(collectionId.id()))
        .thenThrow(new RestException(HttpStatus.NOT_FOUND, "unit test not found error"));

    // getWorkspaceId() should throw an exception for a random collection in this workspace
    MissingObjectException actual =
        assertThrows(
            MissingObjectException.class,
            () -> collectionService.getWorkspaceId(collectionId),
            "for a random collection id");
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  /** workspace is powered by entity service; collection is virtual and will always exist */
  @Test
  void validateCollectionEntityService() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // save this workspace as RAWLS
    WorkspaceRecord workspaceRecord =
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.RAWLS, /* newFlag= */ true);
    workspaceRepository.save(workspaceRecord);

    // note: we do not create any collections here

    // validateCollection() should not throw an error
    assertDoesNotThrow(
        () -> collectionService.validateCollection(CollectionId.of(workspaceId.id())));
  }

  /** workspace is powered by WDS and collection exists */
  @Test
  void validateCollectionWdsExists() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // save this workspace as WDS
    WorkspaceRecord workspaceRecord =
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true);
    workspaceRepository.save(workspaceRecord);

    // create a collection
    CollectionRequestServerModel collectionRequest =
        new CollectionRequestServerModel("name", "description");
    CollectionServerModel savedCollection = collectionService.save(workspaceId, collectionRequest);

    // validateCollection() should not throw an error
    assertDoesNotThrow(
        () -> collectionService.validateCollection(CollectionId.of(savedCollection.getId())));
  }

  /** workspace is powered by WDS; looking for the default collection; collection does not exist */
  @Test
  void validateCollectionWdsNonexistentDefault() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // save this workspace as WDS
    WorkspaceRecord workspaceRecord =
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true);
    workspaceRepository.save(workspaceRecord);

    // note: we do not create any collections here

    // validateCollection() should throw an exception for the default collection id
    MissingObjectException actual =
        assertThrows(
            MissingObjectException.class,
            () -> collectionService.validateCollection(CollectionId.of(workspaceId.id())),
            "for the default collection id");
    assertThat(actual.getMessage()).startsWith("Collection");
  }

  /** looking for a random collection; collection does not exist */
  @Test
  void validateCollectionNonexistentRandom() {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // note: we do not create any collections here

    // since the random collection wasn't found, we couldn't determine its workspace. This will
    // generate a request to Rawls to see if Rawls knows about a workspace whose id is the same
    // as the collection
    when(rawlsClient.getWorkspaceDetails(collectionId.id()))
        .thenThrow(new RestException(HttpStatus.NOT_FOUND, "unit test not found error"));

    // validateCollection() should throw an exception for a random collection in this workspace
    MissingObjectException actual =
        assertThrows(
            MissingObjectException.class,
            () -> collectionService.validateCollection(collectionId),
            "for a random collection id");
    assertThat(actual.getMessage()).startsWith("Collection");
  }
}
