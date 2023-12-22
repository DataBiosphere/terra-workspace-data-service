package org.databiosphere.workspacedataservice.controller;

import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

  /**
   * Handler for MethodArgumentTypeMismatchException. This exception contains the real message we
   * want to display inside the nested cause. This handler ditches the top-level message in favor of
   * that nested one.
   *
   * @param ex the exception in question
   * @param servletRequest the request
   * @return the desired response
   */
  @ExceptionHandler({MethodArgumentTypeMismatchException.class})
  public ResponseEntity<Map<String, Object>> handleAllExceptions(
      Exception ex, HttpServletRequest servletRequest) {
    Map<String, Object> errorBody = new LinkedHashMap<>();

    // TODO AJ-1157: what shape should we return here? This will depend on problem details being
    //    enabled or disabled.
    errorBody.put("timestamp", new Date());
    errorBody.put("status", HttpStatus.BAD_REQUEST.value());
    errorBody.put("error", HttpStatus.BAD_REQUEST.getReasonPhrase());
    errorBody.put("path", servletRequest.getRequestURI());

    // MethodArgumentTypeMismatchException nested exceptions contains
    // the real message we want to display. Gather all the nested messages and display the last one.
    List<String> errorMessages = gatherNestedErrorMessages(ex, new ArrayList<>());
    if (!errorMessages.isEmpty()) {
      errorBody.put("messages", errorMessages.get(errorMessages.size() - 1));
    } else {
      errorBody.put("messages", "Unexpected error: " + ex.getClass().getName());
    }

    return ResponseEntity.badRequest().body(errorBody);
  }

  private List<String> gatherNestedErrorMessages(Throwable t, List<String> accumulator) {
    if (StringUtils.isNotBlank(t.getMessage())) {
      accumulator.add(t.getMessage());
    }
    if (t.getCause() == null) {
      return accumulator;
    }
    return gatherNestedErrorMessages(t.getCause(), accumulator);
  }
}
