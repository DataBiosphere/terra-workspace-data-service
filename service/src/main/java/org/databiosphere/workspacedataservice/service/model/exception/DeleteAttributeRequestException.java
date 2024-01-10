package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class DeleteAttributeRequestException extends RuntimeException {
  public DeleteAttributeRequestException(String message) {
    super(message);
  }
}
