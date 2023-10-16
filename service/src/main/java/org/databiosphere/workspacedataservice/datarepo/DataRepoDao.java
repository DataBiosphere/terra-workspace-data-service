package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotRetrieveIncludeModel;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRepoDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataRepoDao.class);

  private final ClientFactory clientFactory;

  public DataRepoDao(ClientFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  public SnapshotModel getSnapshot(UUID snapshotId) {
    LOGGER.debug("Getting snapshot {}", snapshotId);
    try {
      return clientFactory
          .getRepositoryApi()
          .retrieveSnapshot(snapshotId, List.of(SnapshotRetrieveIncludeModel.TABLES));
    } catch (ApiException e) {
      throw new DataRepoException(e);
    }
  }

  public interface ClientFactory {
    RepositoryApi getRepositoryApi();
  }
}
