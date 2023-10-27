package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.*;
import java.text.SimpleDateFormat;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkspaceManagerDao {
  public static final String INSTANCE_NAME = "terra";
  private final WorkspaceManagerClientFactory workspaceManagerClientFactory;
  private final String workspaceId;
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceManagerDao.class);

  public WorkspaceManagerDao(
      WorkspaceManagerClientFactory workspaceManagerClientFactory, String workspaceId) {
    this.workspaceManagerClientFactory = workspaceManagerClientFactory;
    this.workspaceId = workspaceId;
  }

  /** Creates a snapshot reference in workspaces manager and creates policy linkages. */
  public void createDataRepoSnapshotReference(SnapshotModel snapshotModel) {
    final ReferencedGcpResourceApi resourceApi =
        this.workspaceManagerClientFactory.getReferencedGcpResourceApi(null);

    try {
      String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
      resourceApi.createDataRepoSnapshotReference(
          new CreateDataRepoSnapshotReferenceRequestBody()
              .snapshot(
                  new DataRepoSnapshotAttributes()
                      .instanceName(INSTANCE_NAME)
                      .snapshot(snapshotModel.getId().toString()))
              .metadata(
                  new ReferenceResourceCommonFields()
                      .cloningInstructions(CloningInstructionsEnum.REFERENCE)
                      .name("%s_%s".formatted(snapshotModel.getName(), timeStamp))),
          UUID.fromString(workspaceId));
    } catch (ApiException e) {
      throw new WorkspaceManagerException(e);
    }
  }

  public ResourceList enumerateDataRepoSnapshotReferences() throws ApiException {
    int pageSize = 50; // how many to return from WSM at a time
    int offset = 0;
    ResourceList finalList = new ResourceList(); // collect our results

    UUID workspaceUuid = UUID.fromString(workspaceId);

    while (true) {
      // get a page of results from WSM
      ResourceList thisPage =
          enumerateResources(
              workspaceUuid,
              offset,
              pageSize,
              ResourceType.DATA_REPO_SNAPSHOT,
              StewardshipType.REFERENCED);
      // add this page of results to our collector
      finalList.getResources().addAll(thisPage.getResources());

      if (thisPage.getResources().size() < pageSize) {
        // fewer results from WSM than we requested; this is the last page of results
        return finalList;
      } else {
        // bump our offset and request another page of results
        offset = offset + pageSize;
      }
    }
  }

  /** Retrieves the azure storage container url and sas token for a given workspace. */
  public String getBlobStorageUrl(String storageWorkspaceId, String authToken) {
    final ControlledAzureResourceApi azureResourceApi =
        this.workspaceManagerClientFactory.getAzureResourceApi(authToken);
    int count = 0;
    int maxTries = 3;
    while (true) {
      try {
        UUID workspaceUUID = UUID.fromString(storageWorkspaceId);
        LOGGER.debug(
            "Finding storage resource for workspace {} from Workspace Manager ...", workspaceUUID);
        ResourceList resourceList =
            enumerateResources(workspaceUUID, 0, 5, ResourceType.AZURE_STORAGE_CONTAINER, null);
        // note: it is possible a workspace may have more than one storage container associated with
        // it
        // but currently there is no way to tell which one is the primary except for checking the
        // actual container name
        var storageUUID = extractResourceId(resourceList, storageWorkspaceId);
        if (storageUUID != null) {
          LOGGER.debug(
              "Requesting SAS token-enabled storage url or workspace {} from Workspace Manager ...",
              workspaceUUID);
          CreatedAzureStorageContainerSasToken sasBundle =
              azureResourceApi.createAzureStorageContainerSasToken(
                  workspaceUUID, storageUUID, null, null, null, null);
          return sasBundle.getUrl();
        } else
          throw new ApiException(
              "WorkspaceManagerDao: Can't locate a storage resource matching workspace Id. ");
      } catch (ApiException e) {
        if (++count == maxTries) throw new WorkspaceManagerException(e);
      }
    }
  }

  public UUID extractResourceId(ResourceList resourceList, String storageWorkspaceId) {
    var resourceStorage =
        resourceList.getResources().stream()
            .filter(resource -> resource.getMetadata().getName().contains(storageWorkspaceId))
            .findFirst()
            .orElse(null);
    if (resourceStorage != null) {
      return resourceStorage.getMetadata().getResourceId();
    }
    return null;
  }

  private ResourceList enumerateResources(
      UUID workspaceId,
      int offset,
      int limit,
      ResourceType resourceType,
      StewardshipType stewardshipType)
      throws ApiException {
    ResourceApi resourceApi = this.workspaceManagerClientFactory.getResourceApi(null);
    // TODO: retries
    return resourceApi.enumerateResources(
        workspaceId, offset, limit, resourceType, stewardshipType);
  }
}
