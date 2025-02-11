package org.databiosphere.workspacedataservice.drshub;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.server.ResponseStatusException;

public class DrsHubException extends ResponseStatusException {
  public DrsHubException(RestClientResponseException cause) {
    super(cause.getStatusCode(), cause.getMessage(), cause);
  }

  @VisibleForTesting
  public DrsHubException(HttpStatusCode statusCode, String message) {
    super(statusCode, message);
  }
}
