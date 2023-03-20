package org.databiosphere.workspacedataservice;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;

@ActiveProfiles({"mock-sam","mock-instance-dao"})
@TestPropertySource(properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
@SpringBootTest
public class InstanceInitializerBeanTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;
    @Autowired
    InstanceDao instanceDao;

    String instanceID = "90e1b179-9f83-4a6f-a8c2-db083df4cd03";
    @MockBean
    ManagedIdentityDao mockManagedIdentityDao;
//
//    @MockBean
//    SamDao samDao;
//
//    // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
//    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockManagedIdentityDao.getAzureCredential()
        ).willReturn("aRandomTokenString");
    }

    @Test
    void testHappyPath(){
        assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
        assert(instanceDao.instanceSchemaExists(UUID.fromString(instanceID)));
    }

    @Test
    void testResourceDoesNotExist() throws ApiException {
//        // return the mock ResourcesApi from the mock SamClientFactory
//        given(mockSamClientFactory.getResourcesApi())
//                .willReturn(mockResourcesApi);
//
//        // Call to check permissions in Sam does not throw an exception, but returns false -
//        // i.e. the current user does not have permission
//        given(mockResourcesApi.listResourcesAndPoliciesV2(anyString()))
//                .willReturn(new ArrayList<>());
    }

    @Test
    void testSamError() throws ApiException{
//        // return the mock ResourcesApi from the mock SamClientFactory
//        given(mockSamClientFactory.getResourcesApi())
//                .willReturn(mockResourcesApi);
//        given(mockResourcesApi.listResourcesAndPoliciesV2(anyString()))
//                .willThrow(new ApiException(500, "intentional exception for unit test"));

    }

    @Test
    void testResourceExistsButNotSchema(){

    }

    @Test
    void testSchemaExistsButNotResource(){

    }

    @Test
    void testSchemaCreationError(){

    }

//    @Nested
//    @TestPropertySource(properties = {"twds.instance.workspace-id="})
//    class NoWorkspaceIdTest {
//
//        @Test
//        void workspaceIDNotProvidedNoExceptionThrown() {
//            assertDoesNotThrow(() -> instanceInitializer.onApplicationEvent(new ContextRefreshedEvent(new GenericApplicationContext())));
//        }
//    }
}
