package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.CloningInstructionsEnum;
import bio.terra.workspace.model.CreateDataRepoSnapshotReferenceRequestBody;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ReferenceResourceCommonFields;

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
}
