package org.databiosphere.workspacedataservice.controller;

import com.google.common.annotations.VisibleForTesting;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.BindErrorUtils;

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
      @Nullable Object body,
      @NotNull HttpHeaders headers,
      @NotNull HttpStatusCode statusCode,
      @NotNull WebRequest request) {
    if (body instanceof ProblemDetail problemDetail) {
      Map<String, Object> errorBody = new LinkedHashMap<>();
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
  public ResponseEntity<Map<String, Object>> handleMethodArgMismatch(
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
   * @param ex the exception in question
   * @param headers the response headers
   * @param status the response status
   * @param request the request
   * @return the desired response
   */
  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(
      MethodArgumentNotValidException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {

    // extract nicely-formatted error messages from the exception
    String allErrors = BindErrorUtils.resolveAndJoin(ex.getAllErrors());

    // extract request path from the request
    String path = "";
    if (request instanceof ServletWebRequest servletWebRequest) {
      path = servletWebRequest.getRequest().getRequestURI();
    }

    // extract title
    String title = Objects.requireNonNullElse(ex.getBody().getDetail(), status.toString());

    // build response body
    Object responseBody = buildBody(status.value(), title, allErrors, path);

    return createResponseEntity(responseBody, headers, status, request);
  }

  /**
   * Handler for AuthenticationMaskableException. Rewrites the response to be a 404, using the same
   * message that MissingObjectException would produce. This prevents auth exceptions from leaking
   * existence/non-existence of resources.
   *
   * @param ex the exception in question
   * @param servletRequest the request
   * @return the desired response
   */
  @ExceptionHandler({AuthenticationMaskableException.class})
  public ResponseEntity<Map<String, Object>> maskAuthenticationExceptions(
      AuthenticationMaskableException ex, HttpServletRequest servletRequest) {
    // use the same message that MissingObjectException would produce
    String errorMessage = new MissingObjectException(ex.getObjectType()).getMessage();

    return generateResponse(HttpStatus.NOT_FOUND, errorMessage, servletRequest);
  }

  // helper method for @ExceptionHandlers to build the response payload
  private ResponseEntity<Map<String, Object>> generateResponse(
      HttpStatus httpStatus, String errorMessage, HttpServletRequest servletRequest) {
    Map<String, Object> errorBody =
        buildBody(
            httpStatus.value(),
            httpStatus.getReasonPhrase(),
            errorMessage,
            servletRequest.getRequestURI());

    return ResponseEntity.status(httpStatus)
        .contentType(MediaType.APPLICATION_JSON)
        .body(errorBody);
  }

  // generate a response compatible with the ErrorResponse model
  private Map<String, Object> buildBody(int statusCode, String error, String message, String path) {
    Map<String, Object> errorBody = new LinkedHashMap<>();

    errorBody.put(TIMESTAMP, new Date());
    errorBody.put(STATUS, statusCode);
    errorBody.put(ERROR, error);
    errorBody.put(MESSAGE, message);
    errorBody.put(PATH, path);

    return errorBody;
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
