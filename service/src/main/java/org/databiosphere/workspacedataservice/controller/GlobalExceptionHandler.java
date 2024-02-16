package org.databiosphere.workspacedataservice.controller;

import com.google.common.annotations.VisibleForTesting;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskedException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

  private static final String ERROR = "error";
  private static final String MESSAGE = "message";
  private static final String PATH = "path";
  private static final String STATUS = "status";
  private static final String TIMESTAMP = "timestamp";

  /**
   * Override to explicitly translate Problem Details structures back to {@link
   * org.databiosphere.workspacedata.model.ErrorResponse} structures. This ensures backwards
   * compatibility for clients.
   *
   * <p>Even though {@code spring.mvc.problemdetails.enabled} is set to false in config, many
   * exception classes extend {@link org.springframework.web.server.ResponseStatusException}, and
   * that exception serializes to a problem detail in the response.
   *
   * @param body the body to use for the response
   * @param headers the headers to use for the response
   * @param statusCode the status code to use for the response
   * @param request the current request
   * @return the {@code ResponseEntity} instance to use
   */
  @NotNull
  @Override
  protected ResponseEntity<Object> createResponseEntity(
      Object body,
      @NotNull HttpHeaders headers,
      @NotNull HttpStatusCode statusCode,
      @NotNull WebRequest request) {
    if (body instanceof ProblemDetail problemDetail) {
      Map<String, Object> errorBody = new TreeMap<>();
      errorBody.put(ERROR, problemDetail.getTitle());
      errorBody.put(MESSAGE, problemDetail.getDetail());
      errorBody.put(STATUS, problemDetail.getStatus());
      errorBody.put(TIMESTAMP, new Date());

      if (request instanceof ServletWebRequest servletWebRequest) {
        errorBody.put(PATH, servletWebRequest.getRequest().getRequestURI());
      }
      return new ResponseEntity<>(errorBody, headers, statusCode);
    }

    return super.createResponseEntity(body, headers, statusCode, request);
  }

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

    String errorMessage = "Unexpected error: " + ex.getClass().getName();
    // MethodArgumentTypeMismatchException nested exceptions contains
    // the real message we want to display. Gather all the nested messages and display the last one.
    List<String> errorMessages = gatherNestedErrorMessages(ex, new ArrayList<>());
    if (!errorMessages.isEmpty()) {
      errorMessage = errorMessages.get(errorMessages.size() - 1);
    }
    return generateResponse(HttpStatus.BAD_REQUEST, errorMessage, servletRequest);
  }

  /**
   * Handler for AuthenticationMaskedException. Rewrites the response to be a 404, using the same
   * message that MissingObjectException would produce. This prevents auth exceptions from leaking
   * existence/non-existence of resources.
   *
   * @param ex the exception in question
   * @param servletRequest the request
   * @return the desired response
   */
  @ExceptionHandler({AuthenticationMaskedException.class})
  public ResponseEntity<Map<String, Object>> maskAuthenticationExceptions(
      Exception ex, HttpServletRequest servletRequest) {
    // this cast is safe, given the @ExceptionHandler annotation
    String objectType = ((AuthenticationMaskedException) ex).getObjectType();
    // use the same message that MissingObjectException would produce
    String errorMessage = new MissingObjectException(objectType).getMessage();

    return generateResponse(HttpStatus.NOT_FOUND, errorMessage, servletRequest);
  }

  // helper method for @ExceptionHandlers to build the response payload
  private ResponseEntity<Map<String, Object>> generateResponse(
      HttpStatus httpStatus, String errorMessage, HttpServletRequest servletRequest) {
    Map<String, Object> errorBody = new LinkedHashMap<>();

    errorBody.put(TIMESTAMP, new Date());
    errorBody.put(STATUS, httpStatus.value());
    errorBody.put(ERROR, httpStatus.getReasonPhrase());
    errorBody.put(MESSAGE, errorMessage);
    errorBody.put(PATH, servletRequest.getRequestURI());

    return ResponseEntity.status(httpStatus)
        .contentType(MediaType.APPLICATION_JSON)
        .body(errorBody);
  }

  @VisibleForTesting
  List<String> gatherNestedErrorMessages(@NotNull Throwable t, @NotNull List<String> accumulator) {
    if (StringUtils.isNotBlank(t.getMessage())) {
      accumulator.add(t.getMessage());
    }
    if (t.getCause() == null) {
      return accumulator;
    }
    return gatherNestedErrorMessages(t.getCause(), accumulator);
  }
}
