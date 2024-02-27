package org.databiosphere.workspacedataservice.sam;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.*;

import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
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
class SamDaoInvalidWorkspaceTest extends TestBase {

  @Autowired SamDao samDao;

  @Test
  public void createsFailingDao() {
    assertInstanceOf(MisconfiguredSamDao.class, samDao);
  }

  @Test
  public void permissionsReturnFalse() {
    assertFalse(samDao.hasCreateCollectionPermission());
    assertFalse(samDao.hasCreateCollectionPermission(BearerToken.of("token")));
    assertFalse(samDao.hasDeleteCollectionPermission());
    assertFalse(samDao.hasDeleteCollectionPermission(BearerToken.of("token")));
    assertFalse(samDao.hasWriteWorkspacePermission());
    assertFalse(samDao.hasWriteWorkspacePermission(WorkspaceId.of(randomUUID())));
  }

  @Test
  public void statusThrows() {
    assertThrows(RuntimeException.class, () -> samDao.getSystemStatus());
  }
}
