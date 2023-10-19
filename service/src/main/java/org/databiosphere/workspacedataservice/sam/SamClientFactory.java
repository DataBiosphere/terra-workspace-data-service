package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;

/**
 * interface for SamClientFactory, allowing various implementations: - HttpSamClientFactory, which
 * generates an ApiClient and ResourcesApi from the Sam client library; - MockSamClientFactory,
 * which generates a mock ResourcesApi for unit testing or local development
 */
public interface SamClientFactory {

  ResourcesApi getResourcesApi(String token);

  StatusApi getStatusApi();

  UsersApi getUsersApi(String token);

  GoogleApi getGoogleApi(String token);
}
