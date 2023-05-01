package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class DataRepoController {

    private final DataRepoDao dataRepoDao;

    public DataRepoController(DataRepoDao dataRepoDao) {
        this.dataRepoDao = dataRepoDao;
    }

    @PostMapping("/{instanceId}/snapshots/{version}/{snapshotId}")
    @RetryableApi
    public ResponseEntity<Void> importSnapshot(@PathVariable("instanceId") UUID instanceId,
                                                             @PathVariable("version") String version,
                                                             @PathVariable("snapshotId") UUID snapshotId) {
        boolean allowed = dataRepoDao.hasSnapshotPermission(snapshotId);
        if (allowed){
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } else {
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
    }
}
