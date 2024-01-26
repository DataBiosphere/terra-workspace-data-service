package org.databiosphere.workspacedataservice.leonardo;

import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.springframework.web.server.ResponseStatusException;

public class LeonardoServiceException extends ResponseStatusException {
  public LeonardoServiceException(RestException cause) {
    super(cause.getStatusCode(), cause.getMessage(), cause);
  }
}
