package org.databiosphere.workspacedataservice.rawls;

import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.server.ResponseStatusException;

public class RawlsException extends ResponseStatusException {
  public RawlsException(RestClientResponseException cause) {
    super(cause.getStatusCode(), cause.getMessage(), cause);
  }
}
