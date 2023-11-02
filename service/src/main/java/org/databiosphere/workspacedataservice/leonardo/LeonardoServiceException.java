package org.databiosphere.workspacedataservice.leonardo;

import java.util.Optional;
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class LeonardoServiceException extends ResponseStatusException {
  public LeonardoServiceException(ApiException cause) {
    super(
        Optional.ofNullable(HttpStatus.resolve(cause.getCode()))
            .orElse(HttpStatus.INTERNAL_SERVER_ERROR),
        null,
        cause);
  }

  public LeonardoServiceException(RestException cause) {
    super(cause.getStatus(), cause.getMessage(), cause);
  }

  // public LeonardoServiceException(AuthenticationException cause) {
  //   super(cause.getStatus(), cause.getMessage(), cause);
  // }

  // public LeonardoServiceException(AuthorizationException cause) {
  //   super(cause.getStatus(), cause.getMessage(), cause);
  // }
}
