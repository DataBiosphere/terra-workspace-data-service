package org.databiosphere.workspacedataservice.workspacemanager;

import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.springframework.web.server.ResponseStatusException;

public class WorkspaceManagerException extends ResponseStatusException {
  public WorkspaceManagerException(RestException cause) {
    super(cause.getStatus(), cause.getMessage(), cause);
  }
}
