package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;

/**
 * Mock for the Sam Client ResourcesApi.
 *
 * When returnOnlySuccesses is true, this mock will return true for all permission checks and never throw an exception.
 *
 * When returnOnlySuccesses is false, if the Sam resource id starts with:
 *  - 0-8: true for permission checks, no exception for create/delete requests
 *  - 9: false for permission checks, no exception for create/delete requests
 *  - a: throw ApiException 401
 *  - b: throw ApiException 403
 *  - c: throw ApiException 404
 *  - d: throw ApiException 500
 *  - e: reserved for future use
 *  - f: throw RuntimeException (as a proxy for an unexpected Exception type)
 *
 */
public class MockSamResourcesApi extends ResourcesApi {

    private boolean errorOnPermissionCheck;
    private boolean errorOnResourceCreateDelete;


    public MockSamResourcesApi(boolean errorOnPermissionCheck, boolean errorOnResourceCreateDelete) {
        this.errorOnPermissionCheck = errorOnPermissionCheck;
        this.errorOnResourceCreateDelete = errorOnResourceCreateDelete;
    }

    @Override
    public Boolean resourcePermissionV2(String resourceTypeName, String resourceId, String action) throws ApiException {
        if (errorOnPermissionCheck) {
            maybeThrow(resourceId);
        }
        return !errorOnPermissionCheck || !resourceId.startsWith("9");
    }

    @Override
    public void createResourceV2(String resourceTypeName, CreateResourceRequestV2 resourceCreate) throws ApiException {
        if (errorOnResourceCreateDelete) {
            maybeThrow(resourceCreate.getResourceId());
        }
    }

    @Override
    public void deleteResourceV2(String resourceTypeName, String resourceId) throws ApiException {
        if (errorOnResourceCreateDelete) {
            maybeThrow(resourceId);
        }
    }

    private void maybeThrow(String resourceId) throws ApiException {
        char firstChar = resourceId.charAt(0);
        switch (firstChar) {
            case 'a' -> throw new ApiException(401, "intentional 401 for unit tests");
            case 'b' -> throw new ApiException(403, "intentional 403 for unit tests");
            case 'c' -> throw new ApiException(404, "intentional 404 for unit tests");
            case 'd' -> throw new ApiException(500, "intentional 500 for unit tests");
            case 'e' -> throw new ApiException(0, "intentional 0 for unit tests"); // we get a 0 from connection failures
            case 'f' -> throw new RuntimeException("intentional RuntimeException for unit tests");
        }
    }


}
