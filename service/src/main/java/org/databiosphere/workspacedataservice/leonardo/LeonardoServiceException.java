package org.databiosphere.workspacedataservice.leonardo;

import java.util.Optional;
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
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
}
