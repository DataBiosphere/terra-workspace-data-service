package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockBackupDaoConfig;
import org.databiosphere.workspacedataservice.dao.MockCloneDaoConfig;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.leonardo.LeonardoConfig;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ActiveProfiles({"mock-instance-dao", "mock-backup-dao","mock-clone-dao", "local"})
@TestPropertySource(properties = {"twds.instance.workspace-id="})
@DirtiesContext
@SpringBootTest(classes = {InstanceInitializerConfig.class, MockInstanceDaoConfig.class, MockBackupDaoConfig.class, LeonardoConfig.class, WorkspaceDataServiceConfig.class, MockCloneDaoConfig.class})
class InstanceInitializerNoWorkspaceIdTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;

    @SpyBean
    InstanceDao instanceDao;

    @Test
    void workspaceIDNotProvidedNoExceptionThrown() {
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        //verify that method to create instance was NOT called
        verify(instanceDao, times(0)).createSchema(any());
    }
}
