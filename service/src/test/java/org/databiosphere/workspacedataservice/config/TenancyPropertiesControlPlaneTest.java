package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("control-plane")
class TenancyPropertiesControlPlaneTest {

  @Autowired TwdsProperties twdsProperties;

  @Test
  void allowVirtualCollections() {
    assertTrue(twdsProperties.getTenancy().getAllowVirtualCollections());
  }

  @Test
  void requireEnvWorkspace() {
    assertFalse(twdsProperties.getTenancy().getRequireEnvWorkspace());
  }
}
