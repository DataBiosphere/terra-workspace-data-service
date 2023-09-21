package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

/** Base class for Sam exceptions that are valid to be retried. */
public abstract class SamRetryableException extends SamException {
  protected SamRetryableException(HttpStatus status, String message) {
    super(status, message);
  }
}
