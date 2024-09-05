package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
class TwdsPropertiesControlPlaneTest extends ControlPlaneTestBase {
  @Autowired TwdsProperties twdsProperties;
  @Autowired TenancyProperties tenancyProperties;

  @Test
  void nonNullDataImport() {
    assertNotNull(twdsProperties.dataImportProperties());
  }

  @Test
  void nonNullTenancy() {
    assertNotNull(twdsProperties.tenancyProperties());
  }

  @Test
  void allowVirtualCollections() {
    assertTrue(tenancyProperties.getAllowVirtualCollections());
  }

  @Test
  void requireEnvWorkspace() {
    assertFalse(tenancyProperties.getRequireEnvWorkspace());
  }

  @Test
  void enforceCollectionsMatchWorkspaceId() {
    assertFalse(tenancyProperties.getEnforceCollectionsMatchWorkspaceId());
  }
}
