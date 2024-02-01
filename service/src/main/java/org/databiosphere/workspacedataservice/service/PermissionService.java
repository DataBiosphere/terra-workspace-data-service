package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.model.InstanceId;
import org.databiosphere.workspacedataservice.model.WorkspaceId;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.springframework.stereotype.Component;

@Component
public class PermissionService {

  private final InstanceService instanceService;
  private final SamDao samDao;

  public PermissionService(InstanceService instanceService, SamDao samDao) {
    this.instanceService = instanceService;
    this.samDao = samDao;
  }

  public boolean canReadInstance(InstanceId instanceId, String token) {
    // determine workspace associated with this instance
    WorkspaceId workspaceId = instanceService.getWorkspaceId(instanceId);
    return canReadWorkspace(workspaceId, token);
  }

  public boolean canWriteInstance(InstanceId instanceId, String token) {
    // determine workspace associated with this instance
    WorkspaceId workspaceId = instanceService.getWorkspaceId(instanceId);
    return canWriteWorkspace(workspaceId, token);
  }

  public boolean canReadWorkspace(WorkspaceId workspaceId, String token) {
    // query Sam for permissions
    return samDao.hasReadWorkspacePermission(workspaceId, token);
  }

  public boolean canWriteWorkspace(WorkspaceId workspaceId, String token) {
    // query Sam for permissions
    return samDao.hasWriteWorkspacePermission(workspaceId, token);
  }
}
