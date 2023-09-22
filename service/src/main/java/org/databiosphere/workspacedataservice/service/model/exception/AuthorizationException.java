package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;

public class AuthorizationException extends SamException {

  public AuthorizationException(String message) {
    super(HttpStatus.FORBIDDEN, message);
  }
}
