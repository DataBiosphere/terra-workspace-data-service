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
class SamAuthorizationDaoInvalidWorkspaceIdTest extends TestBase {
  @Autowired SamAuthorizationDao samAuthorizationDao;

  @Test
  void createsFailingDao() {
    assertInstanceOf(MisconfiguredSamAuthorizationDao.class, samAuthorizationDao);
  }

  @Test
  void permissionsReturnFalse() {
    assertFalse(samAuthorizationDao.hasCreateCollectionPermission());
    assertFalse(samAuthorizationDao.hasCreateCollectionPermission(BearerToken.of("token")));
    assertFalse(samAuthorizationDao.hasDeleteCollectionPermission());
    assertFalse(samAuthorizationDao.hasDeleteCollectionPermission(BearerToken.of("token")));
    assertFalse(samAuthorizationDao.hasWriteWorkspacePermission());
    assertFalse(samAuthorizationDao.hasWriteWorkspacePermission(WorkspaceId.of(randomUUID())));
  }
}
