package org.databiosphere.workspacedataservice.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.databiosphere.workspacedataservice.config.DataImportProperties.RecordSinkMode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("control-plane")
class TwdsPropertiesControlPlaneTest {

  @Autowired TwdsProperties twdsProperties;

  @Test
  void nonNullDataImport() {
    assertNotNull(twdsProperties.getDataImport());
  }

  @Test
  void batchWriteRecordSink() {
    assertEquals(RecordSinkMode.RAWLS, twdsProperties.getDataImport().getBatchWriteRecordSink());
  }

  @Test
  void nonNullTenancy() {
    assertNotNull(twdsProperties.getTenancy());
  }

  @Test
  void allowVirtualCollections() {
    assertTrue(twdsProperties.getTenancy().getAllowVirtualCollections());
  }

  @Test
  void requireEnvWorkspace() {
    assertFalse(twdsProperties.getTenancy().getRequireEnvWorkspace());
  }
}
