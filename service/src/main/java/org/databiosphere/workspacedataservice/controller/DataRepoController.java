package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataRepoController {

  private final DataRepoService dataRepoService;
  private final InstanceService instanceService;

  public DataRepoController(DataRepoService dataRepoService, InstanceService instanceService) {
    this.dataRepoService = dataRepoService;
    this.instanceService = instanceService;
  }

  @PostMapping("/{instanceId}/snapshots/{version}/{snapshotId}")
  @RetryableApi
  public ResponseEntity<Void> importSnapshot(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("snapshotId") UUID snapshotId) {
    validateVersion(version);
    instanceService.validateInstance(instanceId);
    dataRepoService.importSnapshot(instanceId, snapshotId);
    return new ResponseEntity<>(HttpStatus.ACCEPTED);
  }
}
