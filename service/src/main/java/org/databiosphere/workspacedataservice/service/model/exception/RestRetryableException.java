package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

/** Base class for REST exceptions that are valid to be retried. */
public abstract class RestRetryableException extends RestException {
  protected RestRetryableException(HttpStatus status, String message) {
    super(status, message);
  }
}
