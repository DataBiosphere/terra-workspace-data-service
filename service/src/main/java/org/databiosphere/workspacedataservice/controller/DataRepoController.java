package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@RestController
public class DataRepoController {

    private final DataRepoService dataRepoService;

    public DataRepoController(DataRepoService dataRepoService) {
        this.dataRepoService = dataRepoService;
    }

    @PostMapping("/{instanceId}/snapshots/{version}/{snapshotId}")
    @RetryableApi
    public ResponseEntity<Void> importSnapshot(@PathVariable("instanceId") UUID instanceId,
                                                             @PathVariable("version") String version,
                                                             @PathVariable("snapshotId") UUID snapshotId) {
        validateVersion(version);
        // TODO: validate the instance, when it's time to actually write anything to that instance
        dataRepoService.importSnapshot(instanceId, snapshotId);
        return new ResponseEntity<>(HttpStatus.ACCEPTED);
    }
}
