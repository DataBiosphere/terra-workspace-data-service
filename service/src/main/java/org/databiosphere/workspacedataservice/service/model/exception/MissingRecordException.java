package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.Arrays;

@ResponseStatus(code = HttpStatus.NOT_FOUND, reason = "Record not found")
public class MissingRecordException extends RuntimeException {
}
