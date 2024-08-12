package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceApi;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@DataPlane
@RestController
public class WorkspaceController implements WorkspaceApi {
  /**
   * POST /workspaces/v1/{workspaceId} : Initialize WDS for a given workspace.
   *
   * @param workspaceId Workspace id (required)
   * @param workspaceInitServerModel The collection to create (required)
   * @return Init request accepted. (status code 202)
   */
  @Override
  public ResponseEntity<GenericJobServerModel> initWorkspaceV1(
      UUID workspaceId, WorkspaceInitServerModel workspaceInitServerModel) {
    // TODO AJ-1951: implement creation of the default collection
    // TODO AJ-1952: implement cloning
    return WorkspaceApi.super.initWorkspaceV1(workspaceId, workspaceInitServerModel);
  }
}
