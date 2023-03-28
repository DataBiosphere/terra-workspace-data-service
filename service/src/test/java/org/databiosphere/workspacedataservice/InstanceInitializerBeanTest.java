package org.databiosphere.workspacedataservice;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ActiveProfiles({"mock-sam","mock-instance-dao", "local"})
@TestPropertySource(properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
@SpringBootTest(classes = {SamConfig.class, MockSamClientFactoryConfig.class, InstanceInitializerConfig.class, MockInstanceDaoConfig.class})
class InstanceInitializerBeanTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @SpyBean
    InstanceDao instanceDao;
    @SpyBean
    SamDao samDao;

    //randomly generated UUID
    UUID instanceID = UUID.fromString("90e1b179-9f83-4a6f-a8c2-db083df4cd03");

    @MockBean
    ManagedIdentityDao mockManagedIdentityDao;
    @MockBean
    SamClientFactory mockSamClientFactory;

    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockManagedIdentityDao.getAzureCredential())
                .willReturn("aRandomTokenString");
        given(mockSamClientFactory.getResourcesApi(any()))
                .willReturn(mockResourcesApi);

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
    void testHappyPath() throws ApiException{
        given(mockResourcesApi.resourcePermissionV2("wds-instance", instanceID.toString(), SamDao.ACTION_READ))
                .willReturn(false);
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        verify(samDao, times(1)).createInstanceResource(any(), any(), any());
        assert(instanceDao.instanceSchemaExists(instanceID));
    }

    @Test
    void testResourceExistsButNotSchema() throws ApiException {
        given(mockResourcesApi.resourcePermissionV2("wds-instance", instanceID.toString(), SamDao.ACTION_READ))
                .willReturn(true);
        instanceInitializerBean.initializeInstance();
        //verify that method to create sam resource was NOT called
        verify(samDao, times(0)).createInstanceResource(any(), any(), any());
        assert(instanceDao.instanceSchemaExists(instanceID));
    }

    @Test
    void testNullToken() {
        given(mockManagedIdentityDao.getAzureCredential()
        ).willReturn(null);
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        //verify that method to create resources was NOT called
        verify(samDao, times(0)).createInstanceResource(any(), any(), any());
        assertFalse(instanceDao.instanceSchemaExists(instanceID));

    }

    @Test
    void testSamException() throws ApiException {
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(instanceID.toString()), anyString()))
                .willThrow(new ApiException(401, "intentional exception for unit test"));
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        //verify that method to create resources was NOT called
        verify(samDao, times(0)).createInstanceResource(any(), any(), any());
        assertFalse(instanceDao.instanceSchemaExists(instanceID));

    }

}
