package org.databiosphere.workspacedataservice.service.model.exception;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class BadStreamingWriteRequestException extends IllegalArgumentException {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BadStreamingWriteRequestException.class);

  public BadStreamingWriteRequestException(IOException ex) {
    super(
        // If the original exception was a JsonMappingException, forward the original message
        // describing the issue to the user.
        // Otherwise, send a generic error message.
        (ex instanceof JsonMappingException jsonMappingException)
            ? jsonMappingException.getOriginalMessage()
            : "The server doesn't understand the request. Please verify you are using the proper format.");
    LOGGER.error("Error parsing request stream ", ex);
  }
}
