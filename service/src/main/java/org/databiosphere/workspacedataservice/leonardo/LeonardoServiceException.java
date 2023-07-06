package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

public class LeonardoServiceException extends ResponseStatusException {
  public LeonardoServiceException(ApiException cause) {
    super(Optional.ofNullable(HttpStatus.resolve(cause.getCode())).orElse(HttpStatus.INTERNAL_SERVER_ERROR), null, cause);
  }
}
