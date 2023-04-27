package org.databiosphere.workspacedataservice.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;


@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler({ MethodArgumentTypeMismatchException.class })
    public ResponseEntity<ProblemDetail> handleAllExceptions(Exception ex, WebRequest request) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        // MethodArgumentTypeMismatchException nested exception contains
        // the real message we want to display
        if (ex.getCause() != null && ex.getCause().getCause() != null) {
            problemDetail.setDetail(ex.getCause().getCause().getMessage());
        }
        return ResponseEntity.badRequest().body(problemDetail);
    }
}
