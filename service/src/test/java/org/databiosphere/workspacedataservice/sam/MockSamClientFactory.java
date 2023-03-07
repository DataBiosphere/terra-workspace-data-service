package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.springframework.beans.factory.annotation.Value;

public class MockSamClientFactory implements SamClientFactory {

    @Value("${errorOnPermissionCheck:false}")
    private boolean errorOnPermissionCheck;
    @Value("${errorOnResourceCreateDelete:false}")
    private boolean errorOnResourceCreateDelete;

    public ResourcesApi getResourcesApi() {
        return new MockSamResourcesApi(errorOnPermissionCheck, errorOnResourceCreateDelete);
    }



}
