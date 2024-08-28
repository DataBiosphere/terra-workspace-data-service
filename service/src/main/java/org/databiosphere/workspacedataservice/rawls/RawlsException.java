package org.databiosphere.workspacedataservice.rawls;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.server.ResponseStatusException;

public class RawlsException extends ResponseStatusException {
  public RawlsException(RestClientResponseException cause) {
    super(cause.getStatusCode(), cause.getMessage(), cause);
  }

  @VisibleForTesting
  public RawlsException(HttpStatusCode statusCode, String message) {
    super(statusCode, message);
  }
}
