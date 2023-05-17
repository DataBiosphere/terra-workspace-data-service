package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotRetrieveIncludeModel;
import bio.terra.datarepo.model.TableModel;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DataRepoDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRepoDao.class);
    public static final String TDRIMPORT_TABLE = "tdr-imports";
    public static final String TDRIMPORT_PRIMARY_KEY = "tablename";

    DataRepoClientFactory dataRepoClientFactory;
    RecordDao recordDao;

    public DataRepoDao(DataRepoClientFactory dataRepoClientFactory, RecordDao recordDao) {
        this.dataRepoClientFactory = dataRepoClientFactory;
        this.recordDao = recordDao;
    }

    public SnapshotModel getSnapshot(UUID snapshotId) {
        LOGGER.debug("Getting snapshot {}", snapshotId);
        try {
            return dataRepoClientFactory.getRepositoryApi().retrieveSnapshot(snapshotId, List.of(SnapshotRetrieveIncludeModel.NONE));
        } catch (ApiException e) {
            throw new DataRepoException(e);
        }
    }

    public void addSnapshot(SnapshotModel snapshot, UUID instanceId) {
        LOGGER.debug("Importing snapshot {}", snapshot.getId());
        RecordType importType = RecordType.valueOf(TDRIMPORT_TABLE);
        if (!recordDao.recordTypeExists(instanceId, importType)){
            recordDao.createRecordType(instanceId, Collections.emptyMap(), importType,
                    new RelationCollection(Collections.emptySet(), Collections.emptySet()),
                    TDRIMPORT_PRIMARY_KEY);
        }
        List<Record> records = new ArrayList<>();
        for (TableModel table : snapshot.getTables()){
            records.add(new Record(table.getName(), importType, RecordAttributes.empty()));
        }
        recordDao.batchUpsert(instanceId, importType, records, Collections.emptyMap());
    }
}
