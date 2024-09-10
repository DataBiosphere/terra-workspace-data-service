package org.databiosphere.workspacedataservice.workspace;

import java.util.Optional;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class RawlsDataTableTypeInspector implements DataTableTypeInspector {

  private final RawlsClient rawlsClient;
  private final WorkspaceRepository workspaceRepository;

  public RawlsDataTableTypeInspector(
      RawlsClient rawlsClient, WorkspaceRepository workspaceRepository) {
    this.rawlsClient = rawlsClient;
    this.workspaceRepository = workspaceRepository;
  }

  /**
   * Determine the type of data tables that the given workspace uses, by making a REST request to
   * Rawls. If Rawls says this is an MC workspace, this method will say the workspace is powered by
   * WDS. Else, this method will say the workspace is powered by Rawls Entity Service.
   *
   * @param workspaceId the workspace to check
   * @return whether the workspace should use WDS-powered or Rawls-powered data tables
   */
  @Override
  public WorkspaceDataTableType getWorkspaceDataTableType(WorkspaceId workspaceId) {

    // try to get from local database first
    Optional<WorkspaceRecord> maybeWorkspaceRecord = workspaceRepository.findById(workspaceId);
    if (maybeWorkspaceRecord.isPresent()) {
      return maybeWorkspaceRecord.get().getDataTableType();
    }

    // if not found in local database, query Rawls to determine the type of workspace
    RawlsWorkspaceDetails details = rawlsClient.getWorkspaceDetails(workspaceId.id());

    WorkspaceDataTableType dataTableType =
        switch (details.workspace().workspaceType()) {
          case MC -> WorkspaceDataTableType.WDS;
          case RAWLS -> WorkspaceDataTableType.RAWLS;
        };

    // persist the Rawls result to the local db for future use
    workspaceRepository.saveWorkspaceRecord(workspaceId, dataTableType);

    return dataTableType;
  }
}
