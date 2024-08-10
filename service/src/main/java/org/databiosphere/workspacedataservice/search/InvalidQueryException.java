package org.databiosphere.workspacedataservice.search;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class InvalidQueryException extends RuntimeException {

  public InvalidQueryException() {
    super("Invalid search query syntax");
  }

  public InvalidQueryException(String message) {
    super(message);
  }

  public InvalidQueryException(String message, Throwable cause) {
    super(message, cause);
  }
}
