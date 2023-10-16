package org.databiosphere.workspacedataservice.datarepo;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import javax.ws.rs.client.Client;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

public class HttpDataRepoDaoFactory implements DataRepoDaoFactory {

  private final String dataRepoUrl;

  public HttpDataRepoDaoFactory(String dataRepoUrl) {
    this.dataRepoUrl = dataRepoUrl;
  }

  public DataRepoDao getDao(String authToken) {
    DataRepoDao.ClientFactory clientFactory = getClientFactory(authToken);
    return new DataRepoDao(clientFactory);
  }

  @Override
  public DataRepoDao.ClientFactory getClientFactory(String authToken) {
    return new HttpDataRepoClientFactory(dataRepoUrl, authToken);
  }

  public class HttpDataRepoClientFactory implements DataRepoDao.ClientFactory {

    private final String dataRepoUrl;
    private final String authToken;
    private final Client commonHttpClient;
    private static final Logger LOGGER =
        LoggerFactory.getLogger(
            org.databiosphere.workspacedataservice.datarepo.HttpDataRepoDaoFactory
                .HttpDataRepoClientFactory.class);

    public HttpDataRepoClientFactory(String dataRepoUrl, String authToken) {
      this.dataRepoUrl = dataRepoUrl;
      this.authToken = authToken;
      this.commonHttpClient = new ApiClient().getHttpClient();
    }

    private ApiClient getApiClient() {
      // create a new data repo client
      ApiClient apiClient = new ApiClient();
      apiClient.setHttpClient(commonHttpClient);

      // initialize the client with the url to data repo
      if (StringUtils.isNotBlank(dataRepoUrl)) {
        apiClient.setBasePath(dataRepoUrl);
      }
      // grab the current user's bearer token (see BearerTokenFilter)
      // TODO: can we eliminate the call to RequestContextHolder too?
      Object token =
          authToken != null
              ? authToken
              : RequestContextHolder.currentRequestAttributes()
                  .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
      // add the user's bearer token to the client
      if (token != null) {
        LOGGER.debug("setting access token for data repo request");
        apiClient.setAccessToken(token.toString());
      } else {
        LOGGER.warn("No access token found for data repo request.");
      }

      // return the client
      return apiClient;
    }

    public RepositoryApi getRepositoryApi() {
      return new RepositoryApi(getApiClient());
    }
  }
}
