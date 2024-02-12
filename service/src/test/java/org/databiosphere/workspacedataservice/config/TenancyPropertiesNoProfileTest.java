package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * when neither the "data-plane" nor "control-plane" profile is active, TenancyProperties are null
 */
@SpringBootTest
@ActiveProfiles("neither-data-plane-nor-control-plane")
class TenancyPropertiesNoProfileTest {

  @Autowired TwdsProperties twdsProperties;

  @Test
  void nullTenancy() {
    assertNull(twdsProperties.getTenancy());
  }
}
