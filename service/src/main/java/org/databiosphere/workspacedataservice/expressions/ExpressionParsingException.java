package org.databiosphere.workspacedataservice.expressions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class ExpressionParsingException extends RuntimeException {
  public ExpressionParsingException(String errorMsg, Throwable cause) {
    super(errorMsg, cause);
  }

  public ExpressionParsingException(String errorMsg) {
    super(errorMsg);
  }
}
