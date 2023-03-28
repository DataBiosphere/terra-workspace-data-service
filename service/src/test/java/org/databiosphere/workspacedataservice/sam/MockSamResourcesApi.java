package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;

/**
 * Mock for the Sam Client ResourcesApi for use in unit tests.
 * Always returns true for all permission checks.
 * Never throws any Exceptions.
 */
public class MockSamResourcesApi extends ResourcesApi {

    @Override
    public Boolean resourcePermissionV2(String resourceTypeName, String resourceId, String action) {
        return true;
    }

    @Override
    public void createResourceV2(String resourceTypeName, CreateResourceRequestV2 resourceCreate) {
        // noop; returns void
    }

    @Override
    public void deleteResourceV2(String resourceTypeName, String resourceId) {
        // noop; returns void
    }

}
