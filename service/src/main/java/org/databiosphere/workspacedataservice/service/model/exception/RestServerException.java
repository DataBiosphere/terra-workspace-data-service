package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

/** Indicates an error response from Sam of a 500, 502, 503, or 504 */
public class RestServerException extends RestRetryableException {
  public RestServerException(HttpStatus status, String message) {
    super(status, message);
  }
}
