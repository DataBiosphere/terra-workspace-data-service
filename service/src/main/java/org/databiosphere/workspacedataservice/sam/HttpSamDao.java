package org.databiosphere.workspacedataservice.sam;

import java.util.List;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

/**
 * Implementation of SamDao that accepts a SamClientFactory, then asks that factory for a new
 * ResourcesApi to use within each method invocation.
 */
public class HttpSamDao implements SamDao {

  protected final SamClientFactory samClientFactory;
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamDao.class);
  private final RestClientRetry restClientRetry;

  HttpSamDao(SamClientFactory samClientFactory, RestClientRetry restClientRetry) {
    this.samClientFactory = samClientFactory;
    this.restClientRetry = restClientRetry;
  }

  // this cache uses token.hashCode as its key. This prevents any logging such as
  // in CacheLogger from logging the raw token.
  @Cacheable(cacheNames = "tokenResolution", key = "#token.hashCode()")
  public String getUserId(BearerToken token) {
    return getUserInfo(token).getUserSubjectId();
  }

  private UserStatusInfo getUserInfo(BearerToken token) {
    LOGGER.debug("Resolving Sam token to UserStatusInfo ...");
    RestCall<UserStatusInfo> samFunction =
        () -> samClientFactory.getUsersApi(token).getUserStatusInfo();
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getUserInfo");
  }

  /**
   * Gets the up/down status of Sam. Using @Cacheable, will reach out to Sam no more than once every
   * 5 minutes (configured in ehcache.xml).
   */
  @Cacheable(cacheNames = "samStatus", key = "'getSystemStatus'")
  public Boolean getSystemStatusOk() {
    return getSystemStatus().getOk();
  }

  public SystemStatus getSystemStatus() {
    RestCall<SystemStatus> samFunction =
        () -> samClientFactory.getStatusApi(BearerToken.empty()).getSystemStatus();
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getSystemStatus");
  }

  public String getPetToken() {
    RestCall<String> samFunction =
        () ->
            samClientFactory
                .getGoogleApi(BearerToken.empty())
                .getArbitraryPetServiceAccountToken(
                    List.of(
                        "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/userinfo.profile"));
    return restClientRetry.withRetryAndErrorHandling(samFunction, "Sam.getPetToken");
  }
}
