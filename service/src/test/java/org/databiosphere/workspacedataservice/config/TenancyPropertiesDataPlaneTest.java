package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("data-plane")
class TenancyPropertiesDataPlaneTest {

  @Autowired TwdsProperties twdsProperties;

  @Test
  void allowVirtualCollections() {
    assertFalse(twdsProperties.getTenancy().getAllowVirtualCollections());
  }

  @Test
  void requireEnvWorkspace() {
    assertTrue(twdsProperties.getTenancy().getRequireEnvWorkspace());
  }
}
