package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles(profiles = "mock-instance-dao")
@DirtiesContext
@SpringBootTest(
    classes = {
      MockInstanceDaoConfig.class,
      SamConfig.class,
      ActivityLoggerConfig.class,
      RestClientRetry.class
    })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"
    }) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
class InstanceServiceSamTest {

  private InstanceService instanceService;

  @Autowired private InstanceDao instanceDao;
  @Autowired private SamDao samDao;
  @Autowired private ActivityLogger activityLogger;

  // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
  @MockBean SamClientFactory mockSamClientFactory;

  // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have
  // to mock it manually
  final ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

  @Value("${twds.instance.workspace-id}")
  String parentWorkspaceId;

  @BeforeEach
  void setUp() throws ApiException {
    instanceService = new InstanceService(instanceDao, samDao, activityLogger);

    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getResourcesApi(null)).willReturn(mockResourcesApi);
    // Sam permission check will always return true
    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(true);
    // clear call history for the mock
    Mockito.clearInvocations(mockResourcesApi);
  }

  @Test
  void createInstanceSamCalls() throws ApiException {
    UUID instanceId = UUID.randomUUID();
    doCreateInstanceTest(instanceId);
  }

  @Test
  void createInstanceWithWorkspaceIdSamCalls() throws ApiException {
    UUID instanceId = UUID.randomUUID();
    UUID workspaceId = UUID.randomUUID();
    doCreateInstanceTest(instanceId);
  }

  @Test
  void deleteInstanceSamCalls() throws ApiException {
    UUID instanceId = UUID.randomUUID();
    doDeleteInstanceTest(instanceId);
  }

  void doCreateInstanceTest(UUID instanceId) throws ApiException {
    // setup: capture order of calls to Sam
    InOrder callOrder = inOrder(mockResourcesApi);

    // call createInstance
    instanceService.createInstance(instanceId, VERSION);

    // createInstance should check permission with Sam exactly once:
    verify(mockResourcesApi, times(1)).resourcePermissionV2(anyString(), anyString(), anyString());

    // createInstance should never call Sam's create-resource API:
    verify(mockResourcesApi, times(0))
        .createResourceV2(anyString(), any(CreateResourceRequestV2.class));

    // the permission call should be first,
    // and that check should be for "write" permission on a workspace with
    // workspaceId=containingWorkspaceId
    callOrder
        .verify(mockResourcesApi)
        .resourcePermissionV2(
            SamDao.RESOURCE_NAME_WORKSPACE, parentWorkspaceId, SamDao.ACTION_WRITE);

    // and that should be the only call we made to Sam
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
    verify(mockResourcesApi, times(1)).resourcePermissionV2(anyString(), anyString(), anyString());

    // deleteInstance should never call Sam's delete-resource API:
    verify(mockResourcesApi, times(0)).deleteResourceV2(anyString(), anyString());

    // the permission call should be first,
    // and that check should be for "write" permission on a workspace with
    // workspaceId=containingWorkspaceId
    callOrder
        .verify(mockResourcesApi)
        .resourcePermissionV2(
            SamDao.RESOURCE_NAME_WORKSPACE, parentWorkspaceId, SamDao.ACTION_DELETE);

    // and that should be the only call we made to Sam
    verifyNoMoreInteractions(mockResourcesApi);
  }
}
