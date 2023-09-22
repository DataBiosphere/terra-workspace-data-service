package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.client.ApiException;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class WorkspaceManagerException extends ResponseStatusException {
  public WorkspaceManagerException(ApiException cause) {
    super(
        Optional.ofNullable(HttpStatus.resolve(cause.getCode()))
            .orElse(HttpStatus.INTERNAL_SERVER_ERROR),
        null,
        cause);
  }
}
