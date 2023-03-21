package org.databiosphere.workspacedataservice;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.UserResourcesResponse;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@ActiveProfiles({"mock-sam","mock-instance-dao", "local"})
@TestPropertySource(properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
@SpringBootTest
public class InstanceInitializerBeanTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @Autowired
    InstanceDao instanceDao;
    @Autowired
    SamDao samDao;

    //randomly generated UUID
    UUID instanceID = UUID.fromString("90e1b179-9f83-4a6f-a8c2-db083df4cd03");

    @MockBean
    ManagedIdentityDao mockManagedIdentityDao;

    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockManagedIdentityDao.getAzureCredential()
        ).willReturn("aRandomTokenString");
    }

    @AfterEach
    void afterEach() {
        // clean up any instances left in the db
        List<UUID> allInstances = instanceDao.listInstanceSchemas();
        allInstances.forEach(instanceId -> {
                    samDao.deleteInstanceResource(instanceId);
                    instanceDao.dropSchema(instanceId);
                });
    }

    @Test
    void testHappyPath(){
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        assert(instanceDao.instanceSchemaExists(instanceID));
    }

    @Test
    void testResourceExistsButNotSchema() throws ApiException {
        given(mockResourcesApi.listResourcesAndPoliciesV2(anyString()))
                .willReturn(Collections.singletonList(new UserResourcesResponse().resourceId(instanceID.toString())));
        instanceInitializerBean.initializeInstance();
        assert(instanceDao.instanceSchemaExists(instanceID));
    }

    @Nested
    @TestPropertySource(properties = {"twds.instance.workspace-id="})
    class NoWorkspaceIdTest {
        @Test
        void workspaceIDNotProvidedNoExceptionThrown() {
            assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        }
    }
}
