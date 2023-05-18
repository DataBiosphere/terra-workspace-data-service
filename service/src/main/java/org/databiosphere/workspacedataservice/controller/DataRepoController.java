package org.databiosphere.workspacedataservice.controller;

import bio.terra.datarepo.model.SnapshotModel;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class DataRepoController {

    private final DataRepoDao dataRepoDao;
    private final WorkspaceManagerDao workspaceManagerDao;

    public DataRepoController(DataRepoDao dataRepoDao, WorkspaceManagerDao workspaceManagerDao) {
        this.dataRepoDao = dataRepoDao;
        this.workspaceManagerDao = workspaceManagerDao;
    }

    @PostMapping("/{instanceId}/snapshots/{version}/{snapshotId}")
    @RetryableApi
    public ResponseEntity<Void> importSnapshot(@PathVariable("instanceId") UUID instanceId,
                                                             @PathVariable("version") String version,
                                                             @PathVariable("snapshotId") UUID snapshotId) {
        // getSnapshot will throw exception is caller does not have access
        SnapshotModel snapshot = dataRepoDao.getSnapshot(snapshotId);

        // createDataRepoSnapshotReference is required to setup policy and will throw exception if policy conflicts
        workspaceManagerDao.createDataRepoSnapshotReference(snapshot);

        // TODO do the import

        return new ResponseEntity<>(HttpStatus.ACCEPTED);
    }
}
