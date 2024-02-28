package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;

/** Mock for SamClientFactory, which returns a MockSamResourcesApi. For use in unit tests. */
public class MockSamClientFactory implements SamClientFactory {

  public ResourcesApi getResourcesApi(BearerToken token) {
    return new MockSamResourcesApi();
  }

  public StatusApi getStatusApi(BearerToken token) {
    return new MockStatusApi();
  }

  @Override
  public UsersApi getUsersApi(BearerToken token) {
    return new MockSamUsersApi();
  }

  @Override
  public GoogleApi getGoogleApi(BearerToken token) {
    return new MockSamGoogleApi();
  }
}
