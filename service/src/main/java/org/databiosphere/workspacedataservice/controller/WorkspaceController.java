package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceApi;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.service.WorkspaceService;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@DataPlane
@ControlPlane
@ConditionalOnProperty(name = "controlPlanePreview", havingValue = "on")
@RestController
public class WorkspaceController implements WorkspaceApi {

  private final PermissionService permissionService;
  private final WorkspaceService workspaceService;

  public WorkspaceController(
      PermissionService permissionService, WorkspaceService workspaceService) {
    this.permissionService = permissionService;
    this.workspaceService = workspaceService;
  }

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
    // require write permission on the workspace
    permissionService.requireWritePermission(WorkspaceId.of(workspaceId));

    // call WorkspaceService and return its result
    GenericJobServerModel initJob =
        workspaceService.initWorkspace(WorkspaceId.of(workspaceId), workspaceInitServerModel);
    return new ResponseEntity<>(initJob, HttpStatus.ACCEPTED);
  }
}
