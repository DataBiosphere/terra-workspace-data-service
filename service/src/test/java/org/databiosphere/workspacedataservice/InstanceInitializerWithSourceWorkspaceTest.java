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

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ActiveProfiles({"mock-sam","mock-instance-dao", "local"})
@SpringBootTest(classes = {SamConfig.class, InstanceInitializerConfig.class, MockInstanceDaoConfig.class})
@TestPropertySource(properties = "twds.instance.source-workspace-id=b6d37280-b983-46be-886d-9beb266f52a1")
class InstanceInitializerWithSourceWorkspaceTest {
    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @SpyBean
    InstanceDao instanceDao;
    @SpyBean
    SamDao samDao;

    //should match the tws.instance.source-workspace-id property on the class
    UUID sourceInstanceID = UUID.fromString("b6d37280-b983-46be-886d-9beb266f52a1");

    @Test
    void sourceWorkspaceSchemaExists() {
        instanceDao.createSchema(sourceInstanceID);
        boolean cloneMode = instanceInitializerBean.isInClodeMode();
        assertFalse(cloneMode);
    }

    @Test
    void sourceWorkspaceIDCorrect() {
        boolean cloneMode = instanceInitializerBean.isInClodeMode();
        assert(cloneMode);
    }

//        @Test
//        void sourceWorkspaceIDInvalid() {
//            boolean cloneMode = instanceInitializerBean.isInClodeMode();
//            assertFalse(cloneMode);
//        }
    }
