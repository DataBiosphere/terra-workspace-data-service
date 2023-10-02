package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.api.ImportApi;
import org.databiosphere.workspacedataservice.generated.model.ImportRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ImportController implements ImportApi {

  @Override
  public ResponseEntity<Void> importV1(UUID instanceUuid, ImportRequest importRequest) {
    // TODO: implementation for imports
    // return TOO_EARLY here as a proof-of-concept that we can override the implementation
    // in ImportApi (which returns NOT_IMPLEMENTED)
    return new ResponseEntity<>(HttpStatus.TOO_EARLY);
  }
}
