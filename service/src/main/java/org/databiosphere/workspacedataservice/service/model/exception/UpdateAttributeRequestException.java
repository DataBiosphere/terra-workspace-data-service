package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class UpdateAttributeRequestException extends RuntimeException {
  public UpdateAttributeRequestException(String message) {
    super(message);
  }
}
