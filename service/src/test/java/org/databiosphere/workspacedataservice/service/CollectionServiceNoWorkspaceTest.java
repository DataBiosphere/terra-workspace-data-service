package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.databiosphere.workspacedataservice.service.model.exception.CollectionException;
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
@TestPropertySource(
    properties = {"twds.instance.workspace-id=", "twds.tenancy.allow-virtual-collections=true"})
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

  @Test
  void testExists() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // exists should be true, even though we are using random ids
    assertTrue(collectionService.exists(workspaceId, collectionId));
  }

  // when twds.instance.workspace-id is empty, collectionService.getWorkspaceId will echo the
  // collectionId back as the workspace id
  @Test
  void getWorkspaceId() {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    assertEquals(WorkspaceId.of(collectionId.id()), collectionService.getWorkspaceId(collectionId));
  }

  @Test
  void createCollectionNotAllowed_virtualCollectionsEnabled() {
    when(tenancyProperties.getAllowVirtualCollections()).thenReturn(true);
    UUID collectionId = UUID.randomUUID();
    var thrown =
        assertThrows(
            CollectionException.class,
            () -> collectionService.createCollection(collectionId, "v0.2"));
    assertThat(thrown)
        .hasMessageContaining("createCollection not allowed when virtual collections are enabled");
  }

  @Test
  void createCollectionNotAllowed_missingWorkspaceId() {
    when(tenancyProperties.getAllowVirtualCollections()).thenReturn(false);
    UUID collectionId = UUID.randomUUID();
    var thrown =
        assertThrows(
            CollectionException.class,
            () -> collectionService.createCollection(collectionId, "v0.2"));
    assertThat(thrown)
        .hasMessageContaining(
            "createCollection requires a workspaceId to be configured or provided");
  }
}
