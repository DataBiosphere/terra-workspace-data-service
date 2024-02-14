package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

// the "data-plane" profile enforces validity of twds.collection.workspace-id, so we need to set
// that
@SpringBootTest(properties = {"twds.collection.workspace-id=00ddba11-0000-0000-0000-000000000000"})
@ActiveProfiles("data-plane")
class TenancyPropertiesDataPlaneTest {

  @Autowired TwdsProperties twdsProperties;

  @Test
  void nonNullTenancy() {
    assertNotNull(twdsProperties.getTenancy());
  }

  @Test
  void allowVirtualCollections() {
    assertFalse(twdsProperties.getTenancy().getAllowVirtualCollections());
  }

  @Test
  void requireEnvWorkspace() {
    assertTrue(twdsProperties.getTenancy().getRequireEnvWorkspace());
  }
}
