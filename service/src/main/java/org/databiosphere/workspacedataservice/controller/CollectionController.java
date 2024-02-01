package org.databiosphere.workspacedataservice.controller;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionsApi;
import org.databiosphere.workspacedataservice.model.WorkspaceId;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CollectionController implements CollectionsApi {

  private final InstanceService instanceService;
  private final PermissionService permissionService;

  public CollectionController(
      InstanceService instanceService, PermissionService permissionService) {
    this.instanceService = instanceService;
    this.permissionService = permissionService;
  }

  @Override
  public ResponseEntity<List<CollectionServerModel>> listCollectionsV1(UUID workspaceUuid) {
    WorkspaceId workspaceId = new WorkspaceId(workspaceUuid);
    // TODO davidan: move permission checks to the service layer, not in the controller?
    // check permission on the instance; under the covers this resolves the workspace
    // associated with the instance and checks permission on that workspace
    BearerToken bearerToken = TokenContextUtil.getToken();
    boolean hasPermission = permissionService.canReadWorkspace(workspaceId, bearerToken.getValue());
    if (!hasPermission) {
      throw new RuntimeException("disallowed!");
    }
    List<CollectionServerModel> colls = instanceService.getCollections(workspaceId);
    return new ResponseEntity<>(colls, HttpStatus.OK);
  }
}
