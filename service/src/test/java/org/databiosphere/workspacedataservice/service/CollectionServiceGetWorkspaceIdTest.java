package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/** Tests for CollectionService.getWorkspaceId() */
@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@SpringBootTest
class CollectionServiceGetWorkspaceIdTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @Autowired @SingleTenant WorkspaceId workspaceId;
  @MockBean private CollectionDao mockCollectionDao;

  @BeforeEach
  void beforeEach() {
    Mockito.reset(mockCollectionDao);
  }

  @Test
  void expectedWorkspaceId() {
    // collection dao returns the workspace id set in $WORKSPACE_ID
    when(mockCollectionDao.getWorkspaceId(any(CollectionId.class))).thenReturn(workspaceId);

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to return the same workspace id that the dao returned
    assertEquals(workspaceId, collectionService.getWorkspaceId(collectionId));
  }

  @Test
  void missingWorkspaceId() {
    // collection dao doesn't find a row and therefore throws EmptyResultDataAccessException
    when(mockCollectionDao.getWorkspaceId(any(CollectionId.class)))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional exception", 1));

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // Act / assert
    MissingObjectException actual =
        assertThrows(
            MissingObjectException.class, () -> collectionService.getWorkspaceId(collectionId));

    // Assert
    assertThat(actual).hasMessageContaining("Collection does not exist");
  }

  @Test
  void nonDefaultWorkspaceId() {
    // collection dao returns a value not equal to the workspace id set in $WORKSPACE_ID
    WorkspaceId mismatchingWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    when(mockCollectionDao.getWorkspaceId(any(CollectionId.class)))
        .thenReturn(mismatchingWorkspaceId);

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to throw, since it found an unexpected workspace id
    assertThrows(CollectionException.class, () -> collectionService.getWorkspaceId(collectionId));
  }
}
