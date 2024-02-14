package org.databiosphere.workspacedataservice.sam;

import static org.junit.jupiter.api.Assertions.*;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

/**
 * When the WORKSPACE_ID env var is not a valid UUID, ensure that all Sam checks return false, and
 * we get an exception from Sam status checks.
 */
@DirtiesContext
@SpringBootTest(
    classes = {SamConfig.class, RestClientRetry.class},
    properties = {"twds.instance.workspace-id=not-a-real-id"})
public class SamDaoInvalidWorkspaceTest {

  @Autowired SamDao samDao;

  @Test
  public void createsFailingDao() {
    assertInstanceOf(MisconfiguredSamDao.class, samDao);
  }

  @Test
  public void permissionsReturnFalse() {
    assertFalse(samDao.hasCreateCollectionPermission());
    assertFalse(samDao.hasCreateCollectionPermission("token"));
    assertFalse(samDao.hasDeleteCollectionPermission());
    assertFalse(samDao.hasDeleteCollectionPermission("token"));
    assertFalse(samDao.hasWriteWorkspacePermission());
    assertFalse(samDao.hasWriteWorkspacePermission("token"));
  }

  @Test
  public void statusThrows() {
    assertThrows(RuntimeException.class, () -> samDao.getSystemStatus());
  }
}
