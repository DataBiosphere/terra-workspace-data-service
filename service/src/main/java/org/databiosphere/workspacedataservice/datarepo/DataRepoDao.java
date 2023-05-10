package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.client.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;

public class DataRepoDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRepoDao.class);

    DataRepoClientFactory dataRepoClientFactory;

    public DataRepoDao(DataRepoClientFactory dataRepoClientFactory) {
        this.dataRepoClientFactory = dataRepoClientFactory;
    }

    public boolean hasSnapshotPermission(UUID snapshotId) throws ApiException {
        LOGGER.debug("Checking for permission for snapshot {} ...", snapshotId);
        try {
            dataRepoClientFactory.getRepositoryApi().retrieveSnapshot(snapshotId, Collections.emptyList());
            //If we get no error, then the user has permission
            return true;
        } catch (ApiException e){
            LOGGER.error(e.getMessage());
            if (e.getCode() == 401 || e.getCode() == 403)
                return false;
            else {
                throw e;
            }
        }
    }

}
