package org.databiosphere.workspacedataservice.sam;

import static org.databiosphere.workspacedataservice.sam.SamAuthorizationDao.ACTION_READ;
import static org.databiosphere.workspacedataservice.sam.SamAuthorizationDao.ACTION_WRITE;

import java.util.Set;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamAuthorizationDaoFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SamAuthorizationDaoFactory.class);

  private final SamClientFactory samClientFactory;
  private final RestClientRetry restClientRetry;

  public SamAuthorizationDaoFactory(
      SamClientFactory samClientFactory, RestClientRetry restClientRetry) {
    this.samClientFactory = samClientFactory;
    this.restClientRetry = restClientRetry;
  }

  public SamAuthorizationDao getSamAuthorizationDao(WorkspaceId workspaceId) {
    LOGGER.debug(
        "Sam integration will query type={}, resourceId={}, actions={}",
        SamAuthorizationDao.RESOURCE_NAME_WORKSPACE,
        workspaceId,
        Set.of(ACTION_READ, ACTION_WRITE));
    return new HttpSamAuthorizationDao(samClientFactory, restClientRetry, workspaceId);
  }
}
