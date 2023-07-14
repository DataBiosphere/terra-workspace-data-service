package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockBackupDao;
import org.databiosphere.workspacedataservice.dao.MockCloneDaoConfig;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.leonardo.LeonardoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceConfig;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ActiveProfiles({"mock-instance-dao", "mock-backup-dao", "mock-clone-dao", "local,", "mock-sam"})
@TestPropertySource(properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
@DirtiesContext
@SpringBootTest(classes = {InstanceInitializerConfig.class, MockInstanceDaoConfig.class, MockBackupDao.class, LeonardoConfig.class, WorkspaceDataServiceConfig.class, MockCloneDaoConfig.class, BackupRestoreService.class, AzureBlobStorage.class, WorkspaceManagerConfig.class, ActivityLoggerConfig.class, SamConfig.class, MockSamClientFactoryConfig.class})
class InstanceInitializerBeanTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @SpyBean
    InstanceDao instanceDao;

    @Value("${twds.instance.workspace-id}")
    String workspaceId;

    //randomly generated UUID
    final UUID instanceID = UUID.fromString("90e1b179-9f83-4a6f-a8c2-db083df4cd03");

    @BeforeEach
    void beforeEach() {
        // clean up any instances left in the db
        List<UUID> allInstances = instanceDao.listInstanceSchemas();
        allInstances.forEach(instanceId -> instanceDao.dropSchema(instanceId));
    }


    @Test
    void testHappyPath() {
        // instance does not exist
        assertFalse(instanceDao.instanceSchemaExists(instanceID));
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        assert(instanceDao.instanceSchemaExists(instanceID));
    }

    @Test
    void testSchemaAlreadyExists() {
        // instance does not exist
        assertFalse(instanceDao.instanceSchemaExists(instanceID));
        // create the instance outside the initializer
        instanceDao.createSchema(instanceID);
        assertTrue(instanceDao.instanceSchemaExists(instanceID));
        // now run the initializer
        instanceInitializerBean.initializeInstance();
        //verify that method to create instance was NOT called again. We expect one call from the setup above.
        verify(instanceDao, times(1)).createSchema(any());
        assertTrue(instanceDao.instanceSchemaExists(instanceID));
    }

    @Test
    void sourceWorkspaceIDNotProvided() {
        boolean cloneMode = instanceInitializerBean.isInCloneMode(null);
        assertFalse(cloneMode);
    }

    @Test
    void blankSourceWorkspaceID() {
        boolean cloneMode = instanceInitializerBean.isInCloneMode("");
        assertFalse(cloneMode);

        cloneMode = instanceInitializerBean.isInCloneMode(" ");
        assertFalse(cloneMode);
    }

    @Test
    void sourceWorkspaceSchemaExists() {
        instanceDao.createSchema(instanceID);
        boolean cloneMode = instanceInitializerBean.isInCloneMode(UUID.randomUUID().toString());
        assertFalse(cloneMode);
    }

    @Test
    void sourceWorkspaceIDCorrect() {
        boolean cloneMode = instanceInitializerBean.isInCloneMode(UUID.randomUUID().toString());
        assert(cloneMode);
    }

    @Test
    void sourceWorkspaceIDInvalid() {
        boolean cloneMode = instanceInitializerBean.isInCloneMode("invalidUUID");
        assertFalse(cloneMode);
    }

    @Test
    void sourceAndCurrentWorkspaceIdsMatch() {
        boolean cloneMode = instanceInitializerBean.isInCloneMode(workspaceId);
        assertFalse(cloneMode);
    }
}
