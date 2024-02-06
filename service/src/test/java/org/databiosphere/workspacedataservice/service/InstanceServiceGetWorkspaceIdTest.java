package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
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

/** Tests for InstanceService.getWorkspaceId() */
@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=4fbac661-2ea2-4592-af6d-3c3f710b0456",
    })
class InstanceServiceGetWorkspaceIdTest {

  @Autowired private InstanceService instanceService;
  @MockBean private InstanceDao mockInstanceDao;

  @Value("${twds.instance.workspace-id:}")
  private String workspaceIdProperty;

  @BeforeEach
  void beforeEach() {
    Mockito.reset(mockInstanceDao);
  }

  @Test
  void expectedWorkspaceId() {
    // instance dao returns the workspace id set in $WORKSPACE_ID
    when(mockInstanceDao.getWorkspaceId(any(InstanceId.class)))
        .thenReturn(WorkspaceId.fromString(workspaceIdProperty));

    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    // expect getWorkspaceId to return the same workspace id that the dao returned
    assertEquals(workspaceIdProperty, instanceService.getWorkspaceId(instanceId).toString());
  }

  @Test
  void missingWorkspaceId() {
    // instance dao doesn't find a row and therefore throws EmptyResultDataAccessException
    when(mockInstanceDao.getWorkspaceId(any(InstanceId.class)))
        .thenThrow(new EmptyResultDataAccessException("unit test intentional exception", 1));

    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    // expect getWorkspaceId to return the instance id as the workspace id; this is a virtual
    // instance
    assertEquals(instanceId.id(), instanceService.getWorkspaceId(instanceId).id());
  }

  @Test
  void unexpectedWorkspaceId() {
    // instance dao returns a value not equal to the workspace id set in $WORKSPACE_ID
    when(mockInstanceDao.getWorkspaceId(any(InstanceId.class)))
        .thenReturn(WorkspaceId.of(UUID.randomUUID()));

    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    // expect getWorkspaceId to throw, since it found an unexpected workspace id
    assertThrows(RuntimeException.class, () -> instanceService.getWorkspaceId(instanceId));
  }
}
