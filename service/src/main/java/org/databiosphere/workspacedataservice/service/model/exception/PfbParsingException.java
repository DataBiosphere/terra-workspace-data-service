package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class PfbParsingException extends IllegalArgumentException {

  public PfbParsingException(String message) {
    super(message);
  }

  public PfbParsingException(String message, Exception e) {
    super(message, e);
  }
}
