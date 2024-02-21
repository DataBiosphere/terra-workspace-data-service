package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.webjars.NotFoundException;

@ResponseStatus(code = HttpStatus.NOT_FOUND)
public class MissingObjectException extends NotFoundException {
  public MissingObjectException(String objectType) {
    super(objectType + " does not exist or you do not have permission to see it");
  }
}
