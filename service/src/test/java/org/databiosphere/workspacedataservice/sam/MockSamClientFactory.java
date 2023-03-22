package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;

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

}
