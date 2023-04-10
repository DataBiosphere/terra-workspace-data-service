package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;

/**
 *  TODO: davidan - temporarily disable all Sam resource operations until AJ-964 and WM-1862 land.
 *  Use this instead of using the real ResourcesApi from the Sam client.
 *  This class returns true for all permission checks
 *  and does not create or delete any Sam resources.
 */
public class DisabledSamResourcesApi extends ResourcesApi {

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
