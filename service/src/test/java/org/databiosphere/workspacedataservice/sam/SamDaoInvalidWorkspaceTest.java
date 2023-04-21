package org.databiosphere.workspacedataservice.sam;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

/**
 * When the WORKSPACE_ID env var is not a valid UUID, ensure
 * that all Sam checks return false, and we get an exception
 * from Sam status checks.
 */
@SpringBootTest(
        classes = {SamConfig.class},
        properties = {"twds.instance.workspace-id=not-a-real-id", "sam.enabled=true"})
public class SamDaoInvalidWorkspaceTest {

    @Autowired
    SamDao samDao;

    @Test
    public void createsFailingDao() {
        assertInstanceOf(MisconfiguredSamDao.class, samDao);
    }

    @Test
    public void permissionsReturnFalse() {
        assertFalse(samDao.hasCreateInstancePermission());
        assertFalse(samDao.hasCreateInstancePermission("token"));
        assertFalse(samDao.hasDeleteInstancePermission());
        assertFalse(samDao.hasDeleteInstancePermission("token"));
        assertFalse(samDao.hasWriteInstancePermission());
        assertFalse(samDao.hasWriteInstancePermission("token"));
    }

    @Test
    public void statusThrows() {
        assertThrows(RuntimeException.class,
                () ->samDao.getSystemStatus());
    }

}
