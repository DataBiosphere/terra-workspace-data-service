package org.databiosphere.workspacedataservice.controller;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;


@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler({ MethodArgumentTypeMismatchException.class })
    public ResponseEntity<Map<String, Object>> handleAllExceptions(Exception ex, HttpServletRequest servletRequest) {
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