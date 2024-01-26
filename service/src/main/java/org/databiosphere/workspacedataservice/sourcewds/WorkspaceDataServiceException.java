package org.databiosphere.workspacedataservice.sourcewds;

import java.util.Optional;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class WorkspaceDataServiceException extends ResponseStatusException {
  public WorkspaceDataServiceException(ApiException cause) {
    super(
        Optional.ofNullable(HttpStatus.resolve(cause.getCode()))
            .orElse(HttpStatus.INTERNAL_SERVER_ERROR),
        null,
        cause);
  }

  public WorkspaceDataServiceException(RestException cause) {
    super(cause.getStatusCode(), cause.getMessage(), cause);
  }
}
