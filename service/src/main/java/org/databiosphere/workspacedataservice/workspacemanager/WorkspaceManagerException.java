package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.client.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class WorkspaceManagerException extends ResponseStatusException {
  public WorkspaceManagerException(ApiException cause) { super(HttpStatus.valueOf(cause.getCode()), null, cause); }
}
