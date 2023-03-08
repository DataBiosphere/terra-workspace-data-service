package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;

public class MockSamClientFactory implements SamClientFactory {
    public ResourcesApi getResourcesApi() {
        return new MockSamResourcesApi();
    }
}
