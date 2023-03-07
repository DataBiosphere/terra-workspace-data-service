package org.databiosphere.workspacedataservice.sam;

import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class MockSamClientFactory implements SamClientFactory {

    private boolean errorOnPermissionCheck;
    private boolean errorOnResourceCreateDelete;


    public MockSamClientFactory(boolean errorOnPermissionCheck, boolean errorOnResourceCreateDelete) {
        this.errorOnPermissionCheck = errorOnPermissionCheck;
        this.errorOnResourceCreateDelete = errorOnResourceCreateDelete;
    }

    public ResourcesApi getResourcesApi() {
        return new MockSamResourcesApi(errorOnPermissionCheck, errorOnResourceCreateDelete);
    }



}
