package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/** Tests for CollectionService.getWorkspaceId() */
@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=4fbac661-2ea2-4592-af6d-3c3f710b0456",
    })
class CollectionServiceGetWorkspaceIdTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @MockBean private CollectionDao mockCollectionDao;

  @Value("${twds.instance.workspace-id:}")
  private String workspaceIdProperty;

  @BeforeEach
  void beforeEach() {
    Mockito.reset(mockCollectionDao);
  }

  @Test
  void expectedWorkspaceId() {
    // collection dao returns the workspace id set in $WORKSPACE_ID
    when(mockCollectionDao.getWorkspaceId(any(CollectionId.class)))
        .thenReturn(WorkspaceId.fromString(workspaceIdProperty));

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to return the same workspace id that the dao returned
    assertEquals(workspaceIdProperty, collectionService.getWorkspaceId(collectionId).toString());
  }

  @Test
  void missingWorkspaceId() {
    // collection dao doesn't find a row and therefore throws EmptyResultDataAccessException
    when(mockCollectionDao.getWorkspaceId(any(CollectionId.class)))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional exception", 1));

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to return the collection id as the workspace id; this is a virtual
    // collection
    assertEquals(collectionId.id(), collectionService.getWorkspaceId(collectionId).id());
  }

  @Test
  void unexpectedWorkspaceId() {
    // collection dao returns a value not equal to the workspace id set in $WORKSPACE_ID
    when(mockCollectionDao.getWorkspaceId(any(CollectionId.class)))
        .thenReturn(WorkspaceId.of(UUID.randomUUID()));

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to throw, since it found an unexpected workspace id
    assertThrows(RuntimeException.class, () -> collectionService.getWorkspaceId(collectionId));
  }
}
