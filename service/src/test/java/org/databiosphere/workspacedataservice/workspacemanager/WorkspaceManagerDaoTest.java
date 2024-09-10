package org.databiosphere.workspacedataservice.workspacemanager;

import static java.util.UUID.randomUUID;
import static org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao.PROP_PURPOSE;
import static org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao.PURPOSE_POLICY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
class WorkspaceManagerDaoTest extends ControlPlaneTestBase {

  @Autowired WorkspaceManagerDao workspaceManagerDao;

  @MockBean WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;

  private WorkspaceId workspaceId;

  final ReferencedGcpResourceApi mockReferencedGcpResourceApi =
      Mockito.mock(ReferencedGcpResourceApi.class);
  final ResourceApi mockResourceApi = Mockito.mock(ResourceApi.class);
  final ControlledAzureResourceApi mockControlledAzureResourceApi =
      Mockito.mock(ControlledAzureResourceApi.class);

  @BeforeEach
  void setUp() {
    workspaceId = WorkspaceId.of(randomUUID());

    given(mockWorkspaceManagerClientFactory.getReferencedGcpResourceApi(nullable(String.class)))
        .willReturn(mockReferencedGcpResourceApi);
    given(mockWorkspaceManagerClientFactory.getResourceApi(nullable(String.class)))
        .willReturn(mockResourceApi);
    given(mockWorkspaceManagerClientFactory.getAzureResourceApi(nullable(String.class)))
        .willReturn(mockControlledAzureResourceApi);
  }

  @Test
  void testSnapshotReturned() throws ApiException {
    final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(randomUUID());
    workspaceManagerDao.linkSnapshotForPolicy(workspaceId, testSnapshot);
    verify(mockReferencedGcpResourceApi)
        .createDataRepoSnapshotReference(
            argThat(
                a ->
                    a.getSnapshot().getSnapshot().equals(testSnapshot.getId().toString())
                        && a.getMetadata()
                            .getCloningInstructions()
                            .equals(CloningInstructionsEnum.REFERENCE)
                        && a.getMetadata().getName().startsWith(testSnapshot.getName())),
            any());
  }

  @Test
  void testErrorThrown() throws ApiException {
    final int statusCode = HttpStatus.UNAUTHORIZED.value();
    final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(randomUUID());
    given(mockReferencedGcpResourceApi.createDataRepoSnapshotReference(any(), any()))
        .willThrow(new ApiException(statusCode, "Intentional error thrown for unit test"));
    var exception =
        assertThrows(
            WorkspaceManagerException.class,
            () -> workspaceManagerDao.linkSnapshotForPolicy(workspaceId, testSnapshot));
    assertEquals(statusCode, exception.getStatusCode().value());
  }

  @Test
  void testResourceReturnSuccess() {
    var resourceUUID =
        buildResourceListObjectAndCallExtraction(
            workspaceId, "sc-" + workspaceId, ResourceType.AZURE_STORAGE_CONTAINER);
    assertEquals(workspaceId.id(), resourceUUID);
  }

  @Test
  void testResourceReturnFailure() {
    var resourceUUID =
        buildResourceListObjectAndCallExtraction(
            workspaceId, "sc-" + randomUUID(), ResourceType.AZURE_STORAGE_CONTAINER);
    assertNull(resourceUUID);
  }

  @Test
  void enumerateResourcesAuthTokenNull() throws ApiException {
    // call enumerateResources with authToken=null
    workspaceManagerDao.enumerateResources(
        WorkspaceId.of(randomUUID()),
        /* offset= */ 0,
        /* limit= */ 10,
        ResourceType.DATA_REPO_SNAPSHOT,
        StewardshipType.REFERENCED,
        /* authToken= */ null);
    // validate argument
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWorkspaceManagerClientFactory).getResourceApi(argumentCaptor.capture());
    String actual = argumentCaptor.getValue();
    assertNull(actual);
  }

  @Test
  void enumerateResourcesAuthTokenPopulated() throws ApiException {
    String expectedAuthToken = RandomStringUtils.randomAlphanumeric(16);
    // call enumerateResources with authToken={random string value}
    workspaceManagerDao.enumerateResources(
        WorkspaceId.of(randomUUID()),
        /* offset= */ 0,
        /* limit= */ 10,
        ResourceType.DATA_REPO_SNAPSHOT,
        StewardshipType.REFERENCED,
        expectedAuthToken);
    // validate argument
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockWorkspaceManagerClientFactory).getResourceApi(argumentCaptor.capture());
    String actualAuthToken = argumentCaptor.getValue();
    assertEquals(expectedAuthToken, actualAuthToken);
  }

  @Test
  void policyOnlyProperty() throws ApiException {
    // set up inputs
    UUID snapshotId = randomUUID();
    SnapshotModel snapshotModel = new SnapshotModel().id(snapshotId);
    // call the create-reference method
    workspaceManagerDao.linkSnapshotForPolicy(workspaceId, snapshotModel);

    // validate that it sent correct Properties to resourceApi.createDataRepoSnapshotReference
    ArgumentCaptor<CreateDataRepoSnapshotReferenceRequestBody> argumentCaptor =
        ArgumentCaptor.forClass(CreateDataRepoSnapshotReferenceRequestBody.class);
    verify(mockReferencedGcpResourceApi)
        .createDataRepoSnapshotReference(argumentCaptor.capture(), any());

    CreateDataRepoSnapshotReferenceRequestBody createBody = argumentCaptor.getValue();
    assertNotNull(createBody.getSnapshot());
    assertEquals(snapshotId.toString(), createBody.getSnapshot().getSnapshot());

    assertNotNull(createBody.getMetadata());
    assertNotNull(createBody.getMetadata().getProperties());
    assertEquals(1, createBody.getMetadata().getProperties().size());
    Property actual = createBody.getMetadata().getProperties().get(0);
    assertEquals(PROP_PURPOSE, actual.getKey());
    assertEquals(PURPOSE_POLICY, actual.getValue());
  }

  @Nullable
  UUID buildResourceListObjectAndCallExtraction(
      WorkspaceId workspaceId, String name, ResourceType type) {
    ResourceList resourceList = new ResourceList();
    ResourceDescription resourceDescription = new ResourceDescription();
    ResourceMetadata meta = new ResourceMetadata();
    resourceDescription.setMetadata(meta);
    resourceDescription.getMetadata().setName(name);
    resourceDescription.getMetadata().setResourceId(workspaceId.id());
    resourceDescription.getMetadata().setResourceType(type);
    List<ResourceDescription> listOfDescriptions = new ArrayList<>();
    listOfDescriptions.add(resourceDescription);
    resourceList.setResources(listOfDescriptions);
    return workspaceManagerDao.extractResourceId(resourceList, workspaceId);
  }
}
