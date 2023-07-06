package org.databiosphere.workspacedataservice.sourcewds;

import org.databiosphere.workspacedata.client.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

public class WorkspaceDataServiceException extends ResponseStatusException {
  public WorkspaceDataServiceException(ApiException cause) {
    super(Optional.ofNullable(HttpStatus.resolve(cause.getCode())).orElse(HttpStatus.INTERNAL_SERVER_ERROR), null, cause);
  }
}
