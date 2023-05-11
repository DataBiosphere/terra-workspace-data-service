package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.client.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class DataRepoException extends ResponseStatusException {
  public DataRepoException(ApiException cause) { super(HttpStatus.valueOf(cause.getCode()), null, cause); }
}
