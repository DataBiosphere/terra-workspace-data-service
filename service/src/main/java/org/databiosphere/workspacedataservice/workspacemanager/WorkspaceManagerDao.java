package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.api.WorkspaceApi;
import bio.terra.workspace.model.*;
import java.text.SimpleDateFormat;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.lang.Nullable;

public class WorkspaceManagerDao {
  public static final String INSTANCE_NAME = "terra";

  /**
   * indicates the purpose of a snapshot reference - e.g. is it created for the sole purpose of
   * linking policies.
   */
  public static final String PROP_PURPOSE = "purpose";

  public static final String PURPOSE_POLICY = "policy";

  private final WorkspaceManagerClientFactory workspaceManagerClientFactory;
  private final RestClientRetry restClientRetry;
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceManagerDao.class);

  public WorkspaceManagerDao(
      WorkspaceManagerClientFactory workspaceManagerClientFactory,
      RestClientRetry restClientRetry) {
    this.workspaceManagerClientFactory = workspaceManagerClientFactory;
    this.restClientRetry = restClientRetry;
  }

  /** Creates a snapshot reference in workspace manager for the sole purpose of policy linkages. */
  public void linkSnapshotForPolicy(WorkspaceId workspaceId, SnapshotModel snapshotModel) {
    Properties properties = null;
    Property policyProperty = new Property();
    policyProperty.setKey(PROP_PURPOSE);
    policyProperty.setValue(PURPOSE_POLICY);
    properties = new Properties();
    properties.add(policyProperty);
    createDataRepoSnapshotReference(workspaceId, snapshotModel, properties);
  }

  /* Creates a snapshot reference in workspace manager. */
  private void createDataRepoSnapshotReference(
      WorkspaceId workspaceId, SnapshotModel snapshotModel, Properties properties) {
    final ReferencedGcpResourceApi resourceApi =
        this.workspaceManagerClientFactory.getReferencedGcpResourceApi(null);
    try {
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
                  workspaceId.id());
      restClientRetry.withRetryAndErrorHandling(
          createSnapshotFunction, "WSM.createSnapshotReference");
    } catch (RestException e) {
      throw new WorkspaceManagerException(e);
    }
  }

  public ResourceList enumerateDataRepoSnapshotReferences(
      WorkspaceId workspaceId, int offset, int limit) {
    // get a page of results from WSM
    return enumerateResources(
        workspaceId,
        offset,
        limit,
        ResourceType.DATA_REPO_SNAPSHOT,
        StewardshipType.REFERENCED,
        /* authToken= */ null);
  }

  /** Retrieves the azure storage container url and sas token for a given workspace. */
  public String getBlobStorageUrl(WorkspaceId storageWorkspaceId, String authToken) {
    try {
      final ControlledAzureResourceApi azureResourceApi =
          this.workspaceManagerClientFactory.getAzureResourceApi(authToken);
      LOGGER.debug(
          "Finding storage resource for workspace {} from Workspace Manager ...",
          storageWorkspaceId);
      ResourceList resourceList =
          enumerateResources(
              storageWorkspaceId,
              /* offset= */ 0,
              /* limit= */ 5,
              ResourceType.AZURE_STORAGE_CONTAINER,
              /* stewardshipType= */ null,
              authToken);
      // note: it is possible a workspace may have more than one storage container associated with
      // it but currently there is no way to tell which one is the primary except for checking the
      // actual container name
      var storageUUID = extractResourceId(resourceList, storageWorkspaceId);
      if (storageUUID != null) {
        LOGGER.debug(
            "Requesting SAS token-enabled storage url or workspace {} from Workspace Manager ...",
            storageWorkspaceId);
        RestCall<CreatedAzureStorageContainerSasToken> sasBundleFunction =
            () ->
                azureResourceApi.createAzureStorageContainerSasToken(
                    storageWorkspaceId.id(),
                    storageUUID,
                    /* sasIpRange= */ null,
                    /* sasExpirationDuration= */ null,
                    /* sasPermissions= */ null,
                    /* sasBlobName= */ null);
        CreatedAzureStorageContainerSasToken sasBundle =
            restClientRetry.withRetryAndErrorHandling(sasBundleFunction, "WSM.sasBundle");
        return sasBundle.getUrl();
      } else {
        throw new RestException(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "WorkspaceManagerDao: Can't locate a storage resource matching workspace Id "
                + storageWorkspaceId);
      }
    } catch (RestException e) {
      throw new WorkspaceManagerException(e);
    }
  }

  @Nullable
  public UUID extractResourceId(ResourceList resourceList, WorkspaceId storageWorkspaceId) {
    var resourceStorage =
        resourceList.getResources().stream()
            .filter(
                resource ->
                    resource.getMetadata().getName().contains(storageWorkspaceId.toString()))
            .findFirst()
            .orElse(null);
    if (resourceStorage != null) {
      return resourceStorage.getMetadata().getResourceId();
    }
    return null;
  }

  ResourceList enumerateResources(
      WorkspaceId workspaceId,
      int offset,
      int limit,
      ResourceType resourceType,
      @Nullable StewardshipType stewardshipType,
      @Nullable String authToken) {
    ResourceApi resourceApi = this.workspaceManagerClientFactory.getResourceApi(authToken);
    try {
      RestCall<ResourceList> enumerateResourcesFunction =
          () ->
              resourceApi.enumerateResources(
                  workspaceId.id(), offset, limit, resourceType, stewardshipType);
      return restClientRetry.withRetryAndErrorHandling(
          enumerateResourcesFunction, "WSM.enumerateResources");
    } catch (RestException e) {
      throw new WorkspaceManagerException(e);
    }
  }

  public WorkspaceDescription getWorkspace(WorkspaceId workspaceId) {
    WorkspaceApi workspaceApi = this.workspaceManagerClientFactory.getWorkspaceApi(null);
    try {
      RestCall<WorkspaceDescription> getWorkspaceFn =
          () -> workspaceApi.getWorkspace(workspaceId.id(), IamRole.READER);
      return restClientRetry.withRetryAndErrorHandling(getWorkspaceFn, "WSM.getWorkspace");
    } catch (RestException e) {
      throw new WorkspaceManagerException(e);
    }
  }
}
