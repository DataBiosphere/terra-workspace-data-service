package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.*;
import java.text.SimpleDateFormat;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkspaceManagerDao {
  public static final String INSTANCE_NAME = "terra";

  /**
   * indicates the purpose of a snapshot reference - e.g. is it created for the sole purpose of
   * linking policies.
   */
  public static final String PROP_PURPOSE = "purpose";

  public static final String PURPOSE_POLICY = "policy";

  private final WorkspaceManagerClientFactory workspaceManagerClientFactory;
  private final String workspaceId;
  private final RestClientRetry restClientRetry;
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceManagerDao.class);

  public WorkspaceManagerDao(
      WorkspaceManagerClientFactory workspaceManagerClientFactory,
      String workspaceId,
      RestClientRetry restClientRetry) {
    this.workspaceManagerClientFactory = workspaceManagerClientFactory;
    this.workspaceId = workspaceId;
    this.restClientRetry = restClientRetry;
  }

  /** Creates a snapshot reference in workspace manager for the sole purpose of policy linkages. */
  public void linkSnapshotForPolicy(SnapshotModel snapshotModel) {
    Properties properties = null;
    Property policyProperty = new Property();
    policyProperty.setKey(PROP_PURPOSE);
    policyProperty.setValue(PURPOSE_POLICY);
    properties = new Properties();
    properties.add(policyProperty);
    createDataRepoSnapshotReference(snapshotModel, properties);
  }

  /* Creates a snapshot reference in workspace manager. */
  private void createDataRepoSnapshotReference(SnapshotModel snapshotModel, Properties properties) {
    final ReferencedGcpResourceApi resourceApi =
        this.workspaceManagerClientFactory.getReferencedGcpResourceApi(null);
    String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
    RestCall<Object> createSnapshotFunction =
        () ->
            resourceApi.createDataRepoSnapshotReference(
                new CreateDataRepoSnapshotReferenceRequestBody()
                    .snapshot(
                        new DataRepoSnapshotAttributes()
                            .instanceName(INSTANCE_NAME)
                            .snapshot(snapshotModel.getId().toString()))
                    .metadata(
                        new ReferenceResourceCommonFields()
                            .cloningInstructions(CloningInstructionsEnum.REFERENCE)
                            .properties(properties)
                            .name("%s_%s".formatted(snapshotModel.getName(), timeStamp))),
                UUID.fromString(workspaceId));
    restClientRetry.withRetryAndErrorHandling(
        createSnapshotFunction, "WSM.createSnapshotReference");
  }

  public ResourceList enumerateDataRepoSnapshotReferences(UUID workspaceId, int offset, int limit)
      throws ApiException {
    // get a page of results from WSM
    return enumerateResources(
        workspaceId,
        offset,
        limit,
        ResourceType.DATA_REPO_SNAPSHOT,
        StewardshipType.REFERENCED,
        null);
  }

  /** Retrieves the azure storage container url and sas token for a given workspace. */
  public String getBlobStorageUrl(String storageWorkspaceId, String authToken) {
    try {
      final ControlledAzureResourceApi azureResourceApi =
          this.workspaceManagerClientFactory.getAzureResourceApi(authToken);
      UUID workspaceUUID = UUID.fromString(storageWorkspaceId);
      LOGGER.debug(
          "Finding storage resource for workspace {} from Workspace Manager ...", workspaceUUID);
      ResourceList resourceList =
          enumerateResources(
              workspaceUUID, 0, 5, ResourceType.AZURE_STORAGE_CONTAINER, null, authToken);
      // note: it is possible a workspace may have more than one storage container associated with
      // it
      // but currently there is no way to tell which one is the primary except for checking the
      // actual container name
      var storageUUID = extractResourceId(resourceList, storageWorkspaceId);
      if (storageUUID != null) {
        LOGGER.debug(
            "Requesting SAS token-enabled storage url or workspace {} from Workspace Manager ...",
            workspaceUUID);
        RestCall<CreatedAzureStorageContainerSasToken> sasBundleFunction =
            () ->
                azureResourceApi.createAzureStorageContainerSasToken(
                    workspaceUUID, storageUUID, null, null, null, null);
        CreatedAzureStorageContainerSasToken sasBundle =
            restClientRetry.withRetryAndErrorHandling(sasBundleFunction, "WSM.sasBundle");
        return sasBundle.getUrl();
      } else {
        throw new ApiException(
            "WorkspaceManagerDao: Can't locate a storage resource matching workspace Id "
                + workspaceUUID.toString());
      }
    } catch (ApiException e) {
      throw new WorkspaceManagerException(e);
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

  ResourceList enumerateResources(
      UUID workspaceId,
      int offset,
      int limit,
      ResourceType resourceType,
      StewardshipType stewardshipType,
      String authToken)
      throws ApiException {
    ResourceApi resourceApi = this.workspaceManagerClientFactory.getResourceApi(authToken);
    RestCall<ResourceList> enumerateResourcesFunction =
        () ->
            resourceApi.enumerateResources(
                workspaceId, offset, limit, resourceType, stewardshipType);
    return restClientRetry.withRetryAndErrorHandling(
        enumerateResourcesFunction, "WSM.enumerateResources");
  }
}
