package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.*;

import java.text.SimpleDateFormat;
import java.util.UUID;


public class WorkspaceManagerDao {
  public static final String INSTANCE_NAME = "terra";
  private final WorkspaceManagerClientFactory workspaceManagerClientFactory;
  private final String workspaceId;
  
  public WorkspaceManagerDao(WorkspaceManagerClientFactory workspaceManagerClientFactory, String workspaceId) {
    this.workspaceManagerClientFactory = workspaceManagerClientFactory;
    this.workspaceId = workspaceId;
  }

  /**
   * Creates a snapshot reference in workspaces manager and creates policy linkages.
   */
  public void createDataRepoSnapshotReference(SnapshotModel snapshotModel) {
    final ReferencedGcpResourceApi resourceApi = this.workspaceManagerClientFactory.getReferencedGcpResourceApi();

    try {
      String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
      resourceApi.createDataRepoSnapshotReference(new CreateDataRepoSnapshotReferenceRequestBody()
                      .snapshot(
                              new DataRepoSnapshotAttributes()
                                      .instanceName(INSTANCE_NAME)
                                      .snapshot(snapshotModel.getId().toString()))
                      .metadata(
                              new ReferenceResourceCommonFields()
                                      .cloningInstructions(CloningInstructionsEnum.REFERENCE)
                                      .name("%s_%s".formatted(snapshotModel.getName(), timeStamp))
                      ),
              UUID.fromString(workspaceId));
    } catch (ApiException e) {
      throw new WorkspaceManagerException(e);
    }
  }

  /**
  Retrieves the azure storage container url and sas token for a given workspace.
   */
  public String getBlobStorageUrl() {
    final ResourceApi resourceApi = this.workspaceManagerClientFactory.getResourceApi();
    final ControlledAzureResourceApi azureResourceApi = this.workspaceManagerClientFactory.getAzureResourceApi();
    try {
      UUID workspaceUUID = UUID.fromString(workspaceId);
      ResourceList resourceList = resourceApi.enumerateResources(workspaceUUID, 0, 5, ResourceType.AZURE_STORAGE_CONTAINER, null);
      // note: it is possible a workspace may have more than one storage container associated with it
      // but currently there is no way to tell which one is the primary except for checking the actual container name
      var storageUUID = extractResourceId(resourceList);
      if(storageUUID != null) {
        CreatedAzureStorageContainerSasToken sasBundle = azureResourceApi.createAzureStorageContainerSasToken(workspaceUUID, storageUUID, null, null, null, null);
        return sasBundle.getUrl();
      }
      else throw new ApiException("Cant locate a storage resource matching workspace Id. ");
    } catch (ApiException e) {
      throw new WorkspaceManagerException(e);
    }
  }

  public UUID extractResourceId(ResourceList resourceList) {
    var resourceStorage = resourceList.getResources().stream().filter(resource -> resource.getMetadata().getName().contains(workspaceId)).findFirst().orElse(null);
    if(resourceStorage != null) {
      return resourceStorage.getMetadata().getResourceId();
    }
    return null;
  }
}