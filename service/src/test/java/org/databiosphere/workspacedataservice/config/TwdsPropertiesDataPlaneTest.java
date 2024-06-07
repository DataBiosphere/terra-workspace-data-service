package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties.RecordSinkMode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("data-plane")
class TwdsPropertiesDataPlaneTest extends TestBase {
  @Autowired TwdsProperties twdsProperties;
  @Autowired DataImportProperties dataImportProperties;
  @Autowired InstanceProperties instanceProperties;
  @Autowired TenancyProperties tenancyProperties;

  @Test
  void nonNullDataImport() {
    assertNotNull(twdsProperties.dataImportProperties());
  }

  @Test
  void batchWriteRecordSink() {
    assertEquals(RecordSinkMode.WDS, dataImportProperties.getBatchWriteRecordSink());
  }

  @Test
  void nonNullTenancy() {
    assertNotNull(twdsProperties.tenancyProperties());
  }

  @Test
  void allowVirtualCollections() {
    assertFalse(tenancyProperties.getAllowVirtualCollections());
  }

  @Test
  void requireEnvWorkspace() {
    assertTrue(tenancyProperties.getRequireEnvWorkspace());
  }

  @Test
  void enforceCollectionsMatchWorkspaceId() {
    assertTrue(tenancyProperties.getEnforceCollectionsMatchWorkspaceId());
  }
}
