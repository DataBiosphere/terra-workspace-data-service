package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;

/**
 * interface for SamClientFactory, allowing various implementations:
 *
 * <ul>
 *   <li>HttpSamClientFactory, which generates an ApiClient and ResourcesApi from the Sam client
 *       library;
 *   <li>MockSamClientFactory, which generates a mock ResourcesApi for unit testing or local
 *       development.
 * </ul>
 */
public interface SamClientFactory {

  // TODO(jladieu): defer creation of SamClientFactory until when it's needed, then create it with
  //   an injected BearerToken; this will let us drop the BearerToken parameter from the API
  ResourcesApi getResourcesApi(BearerToken token);

  StatusApi getStatusApi(BearerToken token);

  UsersApi getUsersApi(BearerToken token);

  GoogleApi getGoogleApi(BearerToken token);
}
