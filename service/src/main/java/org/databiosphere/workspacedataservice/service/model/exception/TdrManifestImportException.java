package org.databiosphere.workspacedataservice.service.model.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.INTERNAL_SERVER_ERROR)
public class TdrManifestImportException extends DataImportException {

  public TdrManifestImportException(String message) {
    super(message);
  }

  public TdrManifestImportException(String message, Throwable t) {
    super(message, t);
  }
}
