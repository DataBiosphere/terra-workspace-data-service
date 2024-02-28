package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamDaoFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SamConfig.class);

  private final SamClientFactory samClientFactory;
  private final RestClientRetry restClientRetry;
  private final BearerTokenHolder bearerTokenHolder;

  public SamDaoFactory(
      SamClientFactory samClientFactory,
      RestClientRetry restClientRetry,
      BearerTokenHolder bearerTokenHolder) {
    this.samClientFactory = samClientFactory;
    this.restClientRetry = restClientRetry;
    this.bearerTokenHolder = bearerTokenHolder;
  }

  public SamDao getSamDao() {
    return new HttpSamDao(samClientFactory, restClientRetry);
  }

  public SamAuthorizationDao getSamAuthorizationDao(WorkspaceId workspaceId) {
    LOGGER.info(
        "Sam integration will query type={}, resourceId={}, action={}",
        SamAuthorizationDao.RESOURCE_NAME_WORKSPACE,
        workspaceId,
        SamAuthorizationDao.ACTION_WRITE);
    return new HttpSamAuthorizationDao(
        samClientFactory, restClientRetry, workspaceId, bearerTokenHolder);
  }
}
