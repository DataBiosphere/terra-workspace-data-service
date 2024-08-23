package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@ActiveProfiles(
    value = {"control-plane"},
    inheritProfiles = false)
@TestPropertySource(properties = {"twds.instance.workspace-id="})
class CollectionServiceNoWorkspaceTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @SpyBean private TenancyProperties tenancyProperties;

  @Value("${twds.instance.workspace-id:}")
  private String workspaceIdProperty;

  @Test
  void assumptions() {
    // ensure the test is set up correctly, with an empty twds.instance.workspace-id property
    assertThat(workspaceIdProperty).isEmpty();
  }

  // when twds.instance.workspace-id is empty, collectionService.getWorkspaceId will echo the
  // collectionId back as the workspace id
  @Test
  void getWorkspaceId() {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    assertEquals(WorkspaceId.of(collectionId.id()), collectionService.getWorkspaceId(collectionId));
  }
}
