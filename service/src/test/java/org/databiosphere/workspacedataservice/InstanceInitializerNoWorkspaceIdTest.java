package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ActiveProfiles({"mock-sam","mock-instance-dao", "local"})
@TestPropertySource(properties = {"twds.instance.workspace-id="})
@SpringBootTest(classes = {SamConfig.class, InstanceInitializerConfig.class, MockInstanceDaoConfig.class})
class InstanceInitializerNoWorkspaceIdTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @SpyBean
    InstanceDao instanceDao;
    @SpyBean
    SamDao samDao;

    @Test
    void workspaceIDNotProvidedNoExceptionThrown() {
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        //verify that method to create resources was NOT called
        verify(samDao, times(0)).createInstanceResource(any(), any(), any());
        verify(instanceDao, times(0)).createSchema(any());
    }
}
