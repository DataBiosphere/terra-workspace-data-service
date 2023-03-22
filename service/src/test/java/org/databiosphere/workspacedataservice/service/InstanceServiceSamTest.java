package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.sam.HttpSamDao;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ActiveProfiles(profiles = "mock-instance-dao")
@SpringBootTest(classes = { MockInstanceDaoConfig.class, SamConfig.class })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InstanceServiceSamTest {

    private InstanceService instanceService;

    @Autowired private InstanceDao instanceDao;
    @Autowired private SamDao samDao;

    // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

    @BeforeEach
    void beforeEach() throws ApiException {
        instanceService = new InstanceService(instanceDao, samDao);

        // return the mock ResourcesApi from the mock SamClientFactory
        given(mockSamClientFactory.getResourcesApi())
                .willReturn(mockResourcesApi);
        // Sam permission check will always return true
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        // clear call history for the mock
        Mockito.clearInvocations(mockResourcesApi);
    }

    @Test
    void createInstanceSamCalls() throws ApiException {
        UUID instanceId = UUID.randomUUID();
        doCreateInstanceTest(instanceId, Optional.empty(), instanceId);
    }

    @Test
    void createInstanceWithWorkspaceIdSamCalls() throws ApiException {
        UUID instanceId = UUID.randomUUID();
        UUID workspaceId = UUID.randomUUID();
        doCreateInstanceTest(instanceId, Optional.of(workspaceId), workspaceId);
    }

    @Test
    void deleteInstanceSamCalls() throws ApiException {
        UUID instanceId = UUID.randomUUID();
        doDeleteInstanceTest(instanceId);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    void doCreateInstanceTest(UUID instanceId, Optional<UUID> workspaceIdInput, UUID expectedWorkspaceId) throws ApiException {
        // setup: capture order of calls to Sam
        InOrder callOrder = inOrder(mockResourcesApi);

        // call createInstance
        instanceService.createInstance(instanceId, VERSION, workspaceIdInput);

        // createInstance should check permission with Sam exactly once:
        verify(mockResourcesApi, times(1))
                .resourcePermissionV2(anyString(), anyString(), anyString());

        // createInstance should also call Sam's create-resource API exactly once:
        verify(mockResourcesApi, times(1))
                .createResourceV2(anyString(), any(CreateResourceRequestV2.class));

        // the permission call should be first,
        // and that check should be for "write" permission on a workspace with workspaceId=expectedWorkspaceId
        callOrder.verify(mockResourcesApi)
                .resourcePermissionV2(SamDao.RESOURCE_NAME_WORKSPACE, expectedWorkspaceId.toString(), SamDao.ACTION_WRITE);

        // the create-resource call should be second,
        // and that call should be for a "wds-instance" resource type with id=instanceid
        ArgumentCaptor<CreateResourceRequestV2> argumentCaptor = ArgumentCaptor.forClass(CreateResourceRequestV2.class);
        callOrder.verify(mockResourcesApi)
                .createResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), argumentCaptor.capture());
        CreateResourceRequestV2 capturedArgument = argumentCaptor.getValue();
        assertEquals(instanceId.toString(), capturedArgument.getResourceId());

        // and those should be the only calls we made to Sam
        verifyNoMoreInteractions(mockResourcesApi);
    }


    void doDeleteInstanceTest(UUID instanceId) throws ApiException {
        // setup: capture order of calls to Sam
        InOrder callOrder = inOrder(mockResourcesApi);

        // bypass Sam and create the instance directly in the db
        instanceDao.createSchema(instanceId);

        // call deleteInstance
        instanceService.deleteInstance(instanceId, VERSION);

        // deleteInstance should check permission with Sam exactly once:
        verify(mockResourcesApi, times(1))
                .resourcePermissionV2(anyString(), anyString(), anyString());

        // deleteInstance should also call Sam's delete-resource API exactly once:
        verify(mockResourcesApi, times(1))
                .deleteResourceV2(anyString(), anyString());

        // the permission call should be first,
        // and that check should be for "delete" permission on a "wds-instance" with id=instanceId
        callOrder.verify(mockResourcesApi)
                .resourcePermissionV2(SamDao.RESOURCE_NAME_INSTANCE, instanceId.toString(), SamDao.ACTION_DELETE);

        // the delete-resource call should be second,
        // and that call should be for a "wds-instance" resource type with id=instanceid
        callOrder.verify(mockResourcesApi)
                .deleteResourceV2(SamDao.RESOURCE_NAME_INSTANCE, instanceId.toString());

        // and those should be the only calls we made to Sam
        verifyNoMoreInteractions(mockResourcesApi);
    }

}
