package org.databiosphere.workspacedataservice.sourcewds;

import okhttp3.OkHttpClient;
import org.databiosphere.workspacedata.api.CloningApi;
import org.databiosphere.workspacedata.client.ApiClient;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.HttpBearerAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Objects;
import java.util.function.Consumer;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class HttpWorkspaceDataServiceClientFactory implements WorkspaceDataServiceClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpWorkspaceDataServiceClientFactory.class);

    public HttpWorkspaceDataServiceClientFactory() {
        // noop
    }

    private ApiClient getApiClient(String token, String workspaceDataServiceUrl) {
        // create a new client
        ApiClient apiClient = new ApiClient();

        // initialize the client with the url to wds endpoint
        if (StringUtils.isNotBlank(workspaceDataServiceUrl)) {
            try {
                LOGGER.info("Setting Wds endpoint url to: {}", workspaceDataServiceUrl);
                URL wdsUrl = new URL(workspaceDataServiceUrl);
                apiClient.setScheme(wdsUrl.getProtocol());
                apiClient.setHost(wdsUrl.getHost());
                if (wdsUrl.getPort() != -1) {
                    apiClient.setPort(wdsUrl.getPort());
                }
                if (StringUtils.isNotBlank(wdsUrl.getPath())) {
                    apiClient.setBasePath(wdsUrl.getPath());
                }
            } catch (MalformedURLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        // grab the current user's bearer token (see BearerTokenFilter)
        if(token.isEmpty()) {
            Object userToken = RequestContextHolder.currentRequestAttributes()
                    .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
            // add the user's bearer token to the client
            if (!Objects.isNull(userToken)) {
                LOGGER.debug("setting access token for workspace data service request");
                addBearerTokenToClient(apiClient, userToken.toString());
            } else {
                LOGGER.warn("No access token found for workspace data service request.");
            }
        }
        else {
            addBearerTokenToClient(apiClient, token);
        }

        return apiClient;
    }

    public CloningApi getBackupClient(String token, String wdsUrl) {
        return new CloningApi(getApiClient(token, wdsUrl));
    }

    private void addBearerTokenToClient(ApiClient apiClient, String token) {
        HttpBearerAuth accessTokenInterceptor = new HttpBearerAuth(token);
        apiClient.setRequestInterceptor(accessTokenInterceptor);
    }

}