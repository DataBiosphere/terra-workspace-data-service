package org.databiosphere.workspacedataservice.service;

import static java.util.UUID.randomUUID;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = "mock-collection-dao")
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CollectionServiceSamTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @Autowired private CollectionDao collectionDao;

  // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
  @MockBean SamClientFactory mockSamClientFactory;

  // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have
  // to mock it manually
  final ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

  @Autowired @SingleTenant WorkspaceId parentWorkspaceId;

  @BeforeEach
  void setUp() throws ApiException {
    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getResourcesApi()).willReturn(mockResourcesApi);
    // Sam permission check will always return true
    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(true);
    // clear call history for the mock
    Mockito.clearInvocations(mockResourcesApi);
  }

  @Test
  void createCollectionSamCalls() throws ApiException {
    doCreateCollectionTest(randomCollectionId());
  }

  @Test
  void createCollectionWithWorkspaceIdSamCalls() throws ApiException {
    doCreateCollectionTest(randomCollectionId());
  }

  @Test
  void deleteCollectionSamCalls() throws ApiException {
    doDeleteCollectionTest(randomCollectionId());
  }

  void doCreateCollectionTest(CollectionId collectionId) throws ApiException {
    // setup: capture order of calls to Sam
    InOrder callOrder = inOrder(mockResourcesApi);

    // call createCollection
    collectionService.createCollection(collectionId.id(), VERSION);

    // createCollection should check permission with Sam exactly once:
    verify(mockResourcesApi, times(1)).resourcePermissionV2(anyString(), anyString(), anyString());

    // createCollection should never call Sam's create-resource API:
    verify(mockResourcesApi, times(0))
        .createResourceV2(anyString(), any(CreateResourceRequestV2.class));

    // the permission call should be first,
    // and that check should be for "write" permission on a workspace with
    // workspaceId=containingWorkspaceId
    callOrder
        .verify(mockResourcesApi)
        .resourcePermissionV2(
            SamAuthorizationDao.RESOURCE_NAME_WORKSPACE,
            parentWorkspaceId.toString(),
            SamAuthorizationDao.ACTION_WRITE);

    // and that should be the only call we made to Sam
    verifyNoMoreInteractions(mockResourcesApi);
  }

  void doDeleteCollectionTest(CollectionId collectionId) throws ApiException {
    // setup: capture order of calls to Sam
    InOrder callOrder = inOrder(mockResourcesApi);

    // bypass Sam and create the collection directly in the db
    collectionDao.createSchema(collectionId);

    // call deleteCollection
    collectionService.deleteCollection(collectionId.id(), VERSION);

    // deleteCollection should check permission with Sam exactly once:
    verify(mockResourcesApi, times(1)).resourcePermissionV2(anyString(), anyString(), anyString());

    // deleteCollection should never call Sam's delete-resource API:
    verify(mockResourcesApi, times(0)).deleteResourceV2(anyString(), anyString());

    // the permission call should be first,
    // and that check should be for "write" permission on a workspace with
    // workspaceId=containingWorkspaceId
    callOrder
        .verify(mockResourcesApi)
        .resourcePermissionV2(
            SamAuthorizationDao.RESOURCE_NAME_WORKSPACE,
            parentWorkspaceId.toString(),
            SamAuthorizationDao.ACTION_WRITE);

    // and that should be the only call we made to Sam
    verifyNoMoreInteractions(mockResourcesApi);
  }

  private CollectionId randomCollectionId() {
    return CollectionId.of(randomUUID());
  }
}
