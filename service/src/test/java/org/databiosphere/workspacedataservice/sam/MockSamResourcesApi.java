package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;

public class MockSamResourcesApi extends ResourcesApi {
    @Override
    public Boolean resourcePermissionV2(String resourceTypeName, String resourceId, String action) throws ApiException {
        return true;
    }

    @Override
    public void createResourceV2(String resourceTypeName, CreateResourceRequestV2 resourceCreate) throws ApiException {
        // noop
    }

    @Override
    public void deleteResourceV2(String resourceTypeName, String resourceId) throws ApiException {
        // noop
    }
}
