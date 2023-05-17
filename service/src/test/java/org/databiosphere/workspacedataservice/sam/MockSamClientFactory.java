package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;

/**
 * Mock for SamClientFactory, which returns a MockSamResourcesApi.
 * For use in unit tests.
 */
public class MockSamClientFactory implements SamClientFactory {
    
    public ResourcesApi getResourcesApi(String token) {
        return new MockSamResourcesApi();
    }

    public StatusApi getStatusApi() {
        return new StatusApi();
    }

    @Override
    public UsersApi getUsersApi(String token) {
        return null;
    }
}
