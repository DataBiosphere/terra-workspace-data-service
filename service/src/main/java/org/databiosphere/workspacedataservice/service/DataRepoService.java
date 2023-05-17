package org.databiosphere.workspacedataservice.service;

import bio.terra.datarepo.model.SnapshotModel;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class DataRepoService {

    private final DataRepoDao dataRepoDao;
    private final WorkspaceManagerDao workspaceManagerDao;
    private final ActivityLogger activityLogger;

    public DataRepoService(DataRepoDao dataRepoDao, WorkspaceManagerDao workspaceManagerDao, ActivityLogger activityLogger) {
        this.dataRepoDao = dataRepoDao;
        this.workspaceManagerDao = workspaceManagerDao;
        this.activityLogger = activityLogger;
    }

    public void importSnapshot(UUID instanceId, UUID snapshotId) {
        // getSnapshot will throw exception is caller does not have access
        SnapshotModel snapshot = dataRepoDao.getSnapshot(snapshotId);

        // createDataRepoSnapshotReference is required to setup policy and will throw exception if policy conflicts
        workspaceManagerDao.createDataRepoSnapshotReference(snapshot);

        activityLogger.saveEventForCurrentUser(event ->
                event.linked().snapshotReference().withUuid(snapshotId));
        // TODO do the import
    }

}
