package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.client.ApiException;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class DataRepoException extends ResponseStatusException {
  public DataRepoException(ApiException cause) {
    super(
        Optional.ofNullable(HttpStatus.resolve(cause.getCode()))
            .orElse(HttpStatus.INTERNAL_SERVER_ERROR),
        null,
        cause);
  }
}
