package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.INTERNAL_SERVER_ERROR)
public class InstanceException extends RuntimeException {

  public InstanceException(String message) {
    super(message);
  }

  public InstanceException(String message, Exception e) {
    super(message, e);
  }
}
