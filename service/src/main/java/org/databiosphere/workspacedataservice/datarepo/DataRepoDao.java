package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotRetrieveIncludeModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class DataRepoDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRepoDao.class);

    final DataRepoClientFactory dataRepoClientFactory;

    public DataRepoDao(DataRepoClientFactory dataRepoClientFactory) {
        this.dataRepoClientFactory = dataRepoClientFactory;
    }

    public SnapshotModel getSnapshot(UUID snapshotId) {
        LOGGER.debug("Getting snapshot {}", snapshotId);
        try {
            return dataRepoClientFactory.getRepositoryApi().retrieveSnapshot(snapshotId, List.of(SnapshotRetrieveIncludeModel.NONE));
        } catch (ApiException e) {
            throw new DataRepoException(e);
        }
    }
}
