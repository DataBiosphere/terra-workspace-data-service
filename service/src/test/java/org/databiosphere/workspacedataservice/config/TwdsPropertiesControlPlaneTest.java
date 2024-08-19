package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ActiveProfiles("control-plane")
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
@DirtiesContext
class TwdsPropertiesControlPlaneTest {
  @Autowired TwdsProperties twdsProperties;
  @Autowired DataImportProperties dataImportProperties;
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
