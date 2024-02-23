package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.InstanceProperties.SingleTenant;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceIdDao;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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

  @Autowired @SingleTenant private WorkspaceId workspaceId;

  @MockBean private WorkspaceIdDao mockWorkspaceIdDao;
  // although not used directly in this test, since in the dataplane this is implemented by
  // PostgresCollectionDao, which also implements WorkspaceIdDao, which is mocked here, we must
  // also provide a mock for the collectionDao
  @MockBean private CollectionDao mockCollectionDao;

  @Test
  void expectedWorkspaceId() {
    // collection dao returns the workspace id set in $WORKSPACE_ID
    when(mockWorkspaceIdDao.getWorkspaceId(any(CollectionId.class))).thenReturn(workspaceId);

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to return the same workspace id that the dao returned
    assertEquals(workspaceId, collectionService.getWorkspaceId(collectionId));
  }

  @Test
  void missingWorkspaceId() {
    // collection dao doesn't find a row and therefore throws EmptyResultDataAccessException
    when(mockWorkspaceIdDao.getWorkspaceId(any(CollectionId.class)))
        .thenThrow(new MissingObjectException("unit test intentional exception"));

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // bubble up the exception from the dao, this should never happen
    assertThrows(
        MissingObjectException.class, () -> collectionService.getWorkspaceId(collectionId).id());
  }

  @Test
  void unexpectedWorkspaceId() {
    // collection dao returns a value not equal to the workspace id set in $WORKSPACE_ID
    when(mockWorkspaceIdDao.getWorkspaceId(any(CollectionId.class)))
        .thenReturn(WorkspaceId.of(UUID.randomUUID()));

    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    // expect getWorkspaceId to throw, since it found an unexpected workspace id
    assertThrows(CollectionException.class, () -> collectionService.getWorkspaceId(collectionId));
  }
}
