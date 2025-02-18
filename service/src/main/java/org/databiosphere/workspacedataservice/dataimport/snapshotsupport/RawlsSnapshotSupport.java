package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class RawlsSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final RawlsClient rawlsClient;
  private final ActivityLogger activityLogger;

  public RawlsSnapshotSupport(
      WorkspaceId workspaceId, RawlsClient rawlsClient, ActivityLogger activityLogger) {
    this.workspaceId = workspaceId;
    this.rawlsClient = rawlsClient;
    this.activityLogger = activityLogger;
  }

  @Override
  protected ResourceList enumerateDataRepoSnapshotReferences(int offset, int pageSize) {
    SnapshotListResponse snapshotListResponse =
        rawlsClient.enumerateDataRepoSnapshotReferences(workspaceId.id(), offset, pageSize);

    return toResourceList(snapshotListResponse.gcpDataRepoSnapshots());
  }

  @Override
  protected void linkSnapshot(UUID snapshotId) {
    rawlsClient.createSnapshotReference(workspaceId.id(), snapshotId);
    activityLogger.saveEventForCurrentUser(
        user -> user.linked().snapshotReference().withUuid(snapshotId));
  }

  /**
   * Transforms a List<DataRepoSnapshotResource> to a ResourceList. RawlsSnapshotSupport and
   * WsmSnapshotSupport return slightly different models from their API calls; performing this
   * translation allows the two SnapshotSupport implementations to share code elsewhere.
   *
   * @param dataRepoSnapshotResourceList the model returned by Rawls APIs
   * @return the model returned by WSM APIs
   */
  private ResourceList toResourceList(List<DataRepoSnapshotResource> dataRepoSnapshotResourceList) {
    // transform the input List<DataRepoSnapshotResource> to List<ResourceDescription>
    List<ResourceDescription> resources =
        dataRepoSnapshotResourceList.stream()
            .map(
                dataRepoSnapshotResource -> {
                  ResourceAttributesUnion resourceAttributesUnion = new ResourceAttributesUnion();
                  resourceAttributesUnion.setGcpDataRepoSnapshot(
                      dataRepoSnapshotResource.getAttributes());

                  ResourceDescription resourceDescription = new ResourceDescription();
                  resourceDescription.setMetadata(dataRepoSnapshotResource.getMetadata());
                  resourceDescription.setResourceAttributes(resourceAttributesUnion);
                  return resourceDescription;
                })
            .toList();

    // wrap the List<ResourceDescription> inside a ResourceList
    ResourceList resourceList = new ResourceList();
    resourceList.setResources(resources);

    // and return
    return resourceList;
  }
}
