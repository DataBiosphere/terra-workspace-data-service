package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.CONFLICT)
public class AttributeExistsException extends UpdateAttributeRequestException {
  public AttributeExistsException(String message) {
    super(message);
  }
}
