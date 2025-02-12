package org.databiosphere.workspacedataservice.drshub;

import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.server.ResponseStatusException;

public class DrsHubException extends ResponseStatusException {
  public DrsHubException(RestClientResponseException cause) {
    super(cause.getStatusCode(), cause.getMessage(), cause);
  }
}
