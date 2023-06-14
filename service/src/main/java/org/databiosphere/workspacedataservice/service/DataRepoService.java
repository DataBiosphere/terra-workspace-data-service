package org.databiosphere.workspacedataservice.service;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.TableModel;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class DataRepoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRepoService.class);

    private final DataRepoDao dataRepoDao;
    private final WorkspaceManagerDao workspaceManagerDao;
    private final ActivityLogger activityLogger;
    private final RecordDao recordDao;

    public static final String TDRIMPORT_TABLE = "tdr-imports";
    public static final String TDRIMPORT_PRIMARY_KEY = "tablename";
    public static final String TDRIMPORT_SNAPSHOT_ID = "Snapshot id";
    public static final String TDRIMPORT_IMPORT_TIME = "Import Time";
    public DataRepoService(DataRepoDao dataRepoDao, WorkspaceManagerDao workspaceManagerDao, ActivityLogger activityLogger, RecordDao recordDao) {
        this.dataRepoDao = dataRepoDao;
        this.workspaceManagerDao = workspaceManagerDao;
        this.activityLogger = activityLogger;
        this.recordDao = recordDao;
    }

    public void importSnapshot(UUID instanceId, UUID snapshotId) {
        // getSnapshot will throw exception if caller does not have access
        SnapshotModel snapshot = dataRepoDao.getSnapshot(snapshotId);

        // createDataRepoSnapshotReference is required to setup policy and will throw exception if policy conflicts
        workspaceManagerDao.createDataRepoSnapshotReference(snapshot);
        activityLogger.saveEventForCurrentUser(user ->
                user.linked().snapshotReference().withUuid(snapshotId));

        //do the import
        addSnapshot(snapshot, instanceId);
    }

    public void addSnapshot(SnapshotModel snapshot, UUID instanceId) {
        LOGGER.debug("Importing snapshot {}", snapshot.getId());
        RecordType importType = RecordType.valueOf(TDRIMPORT_TABLE);
        Map<String, DataTypeMapping> schema = Map.of(TDRIMPORT_SNAPSHOT_ID, DataTypeMapping.STRING, TDRIMPORT_IMPORT_TIME, DataTypeMapping.DATE_TIME);
        if (!recordDao.recordTypeExists(instanceId, importType)){
            recordDao.createRecordType(instanceId, schema, importType,
                    new RelationCollection(Collections.emptySet(), Collections.emptySet()),
                    TDRIMPORT_PRIMARY_KEY);
            activityLogger.saveEventForCurrentUser(user ->
                    user.created().table().withId(snapshot.getName()));
        }
        List<Record> records = new ArrayList<>();
        for (TableModel table : snapshot.getTables()){
            records.add(new Record(table.getName(), importType, RecordAttributes.empty().putAttribute(TDRIMPORT_SNAPSHOT_ID, snapshot.getId()).putAttribute(TDRIMPORT_IMPORT_TIME, LocalDateTime.now())));
        }
        recordDao.batchUpsert(instanceId, importType, records, schema);
        if (!records.isEmpty()) {
            activityLogger.saveEventForCurrentUser(user ->
                    user.created().record().withRecordType(importType).ofQuantity(records.size()));
        }
    }
}
