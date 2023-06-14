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
  public CreatedAzureStorageContainerSasToken getBlobStorageUrl() {
    final ResourceApi resourceApi = this.workspaceManagerClientFactory.getResourceApi();
    final ControlledAzureResourceApi azureResourceApi = this.workspaceManagerClientFactory.getAzureResourceApi();
    try {
      UUID workspace_UUID = UUID.fromString(workspaceId);
      ResourceList resourceList = resourceApi.enumerateResources(workspace_UUID, 0, 10, null, null);
      UUID storageUUID = resourceList.getResources().get(0).getMetadata().getResourceId();

      CreatedAzureStorageContainerSasToken sasBundle = azureResourceApi.createAzureStorageContainerSasToken(workspace_UUID, storageUUID, null, null, null, null);
      return sasBundle;
    } catch (ApiException e) {
      throw new WorkspaceManagerException(e);
    }
  }
}