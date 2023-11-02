package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

/**
 * Indicates a failure to connect to Sam, typically a timeout, and generally due to transient
 * network or connection issues.
 */
public class RestConnectionException extends RestRetryableException {

  public RestConnectionException() {
    super(HttpStatus.INTERNAL_SERVER_ERROR, "Could not connect to Sam");
  }

  public RestConnectionException(HttpStatus status, String message) {
    super(status, message);
  }
}
