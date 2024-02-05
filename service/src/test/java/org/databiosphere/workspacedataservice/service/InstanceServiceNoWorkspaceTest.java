package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@TestPropertySource(properties = {"twds.instance.workspace-id="})
class InstanceServiceNoWorkspaceTest {

  @Autowired private InstanceService instanceService;

  @Value("${twds.instance.workspace-id:}")
  private String workspaceIdProperty;

  @Test
  void assumptions() {
    // ensure the test is set up correctly, with an empty twds.instance.workspace-id property
    assertThat(workspaceIdProperty).isEmpty();
  }

  // when twds.instance.workspace-id is empty, instanceService.getWorkspaceId will echo the
  // instanceid back as the workspace id
  @Test
  void getWorkspaceId() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    assertEquals(WorkspaceId.of(instanceId.id()), instanceService.getWorkspaceId(instanceId));
  }
}
