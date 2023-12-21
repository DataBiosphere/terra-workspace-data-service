package org.databiosphere.workspacedataservice.controller;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
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

    errorBody.put("timestamp", new Date());
    errorBody.put("status", HttpStatus.BAD_REQUEST.value());
    errorBody.put("error", HttpStatus.BAD_REQUEST.getReasonPhrase());
    // MethodArgumentTypeMismatchException nested exception contains
    // the real message we want to display
    if (ex.getCause() != null && ex.getCause().getCause() != null) {
      errorBody.put("message", ex.getCause().getCause().getMessage());
    }
    errorBody.put("path", servletRequest.getRequestURI());

    return ResponseEntity.badRequest().body(errorBody);
  }
}
