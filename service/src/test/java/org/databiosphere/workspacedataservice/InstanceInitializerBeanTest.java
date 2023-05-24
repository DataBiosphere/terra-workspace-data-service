package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ActiveProfiles({"mock-instance-dao", "local"})
@TestPropertySource(properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
@DirtiesContext
@SpringBootTest(classes = {InstanceInitializerConfig.class, MockInstanceDaoConfig.class})
class InstanceInitializerBeanTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @SpyBean
    InstanceDao instanceDao;

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

}
