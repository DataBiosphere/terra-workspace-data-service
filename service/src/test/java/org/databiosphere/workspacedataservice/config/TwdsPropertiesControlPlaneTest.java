package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.databiosphere.workspacedataservice.config.DataImportProperties.RecordSinkMode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ActiveProfiles(profiles = {"control-plane", "test"})
@TestPropertySource(
    properties = {
      // TODO(AJ-1656): control-plane should not require instance config in any form, this is a hold
      //   over from direct injection of @Value('twds.instance.workspace-id')
      "twds.instance.workspace-id=",
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
  void batchWriteRecordSink() {
    assertEquals(RecordSinkMode.RAWLS, dataImportProperties.getBatchWriteRecordSink());
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
