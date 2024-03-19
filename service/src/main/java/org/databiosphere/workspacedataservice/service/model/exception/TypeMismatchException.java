package org.databiosphere.workspacedataservice.service.model.exception;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.BAD_REQUEST)
public class TypeMismatchException extends IllegalArgumentException {

  public TypeMismatchException(List<String> errorInfo) {
    super(
        "Some of the records in your request don't have the proper data for the record type. "
            + "This is likely not an exhaustive list so please look for records with similar problems in your request: "
            + errorInfo.subList(0, Math.min(100, errorInfo.size())));
  }
}
