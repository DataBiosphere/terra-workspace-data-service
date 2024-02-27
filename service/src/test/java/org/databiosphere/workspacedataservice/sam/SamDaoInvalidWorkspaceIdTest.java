package org.databiosphere.workspacedataservice.sam;

import static org.junit.jupiter.api.Assertions.*;

import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * When the WORKSPACE_ID env var is not a valid UUID, ensure that all Sam checks return false, and
 * we get an exception from Sam status checks.
 */
@DirtiesContext
@SpringBootTest(classes = {SamConfig.class, RestClientRetry.class})
@TestPropertySource(
    properties = {
      // explicitly set the workspace-id to something invalid
      "twds.instance.workspace-id=not-a-real-id",
    })
class SamDaoInvalidWorkspaceIdTest extends TestBase {

  @Autowired SamDao samDao;

  @Test
  public void statusThrows() {
    assertThrows(RuntimeException.class, () -> samDao.getSystemStatus());
  }
}
