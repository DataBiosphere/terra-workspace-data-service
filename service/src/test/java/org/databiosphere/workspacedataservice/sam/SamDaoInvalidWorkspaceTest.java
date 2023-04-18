package org.databiosphere.workspacedataservice.sam;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

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

    private final UUID someUuid = UUID.randomUUID();

    @Test
    public void createsFailingDao() {
        assertInstanceOf(FailingSamDao.class, samDao);
    }

    @Test
    public void permissionsReturnFalse() {
        assertFalse(samDao.hasCreateInstancePermission(someUuid));
        assertFalse(samDao.hasCreateInstancePermission(someUuid, "token"));
        assertFalse(samDao.hasDeleteInstancePermission(someUuid));
        assertFalse(samDao.hasDeleteInstancePermission(someUuid, "token"));
        assertFalse(samDao.hasWriteInstancePermission(someUuid));
        assertFalse(samDao.hasWriteInstancePermission(someUuid, "token"));
    }

    @Test
    public void statusThrows() {
        assertThrows(RuntimeException.class,
                () ->samDao.getSystemStatus());
    }

}
