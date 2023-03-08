package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;

import java.util.Collections;
import java.util.UUID;


public class HttpSamDao extends HttpSamClientSupport implements SamDao {

    private final SamClientFactory samClientFactory;

    public HttpSamDao(SamClientFactory samClientFactory) {
        this.samClientFactory = samClientFactory;

    }

    @Override
    public boolean hasCreateInstancePermission(UUID parentWorkspaceId) {
        return hasPermission(RESOURCE_NAME_WORKSPACE, parentWorkspaceId.toString(), ACTION_WRITE,
                "hasCreateInstancePermission");
    }

    @Override
    public boolean hasDeleteInstancePermission(UUID instanceId) {
        return hasPermission(RESOURCE_NAME_INSTANCE, instanceId.toString(), ACTION_DELETE,
                "hasDeleteInstancePermission");
    }

    private boolean hasPermission(String resourceType, String resourceId, String action, String loggerHint) {
        SamFunction<Boolean> samFunction = () -> samClientFactory.getResourcesApi()
                .resourcePermissionV2(resourceType, resourceId, action);
        return executeSamRequest(samFunction, loggerHint);
    }

    @Override
    public void createInstanceResource(UUID instanceId, UUID parentWorkspaceId) {
        FullyQualifiedResourceId parent = new FullyQualifiedResourceId();
        parent.setResourceTypeName(RESOURCE_NAME_WORKSPACE);
        parent.setResourceId(parentWorkspaceId.toString());

        CreateResourceRequestV2 createResourceRequest = new CreateResourceRequestV2();
        createResourceRequest.setResourceId(instanceId.toString());
        createResourceRequest.setParent(parent);
        createResourceRequest.setAuthDomain(Collections.emptyList());

        VoidSamFunction samFunction = () -> samClientFactory.getResourcesApi().createResourceV2(RESOURCE_NAME_INSTANCE, createResourceRequest);
        executeSamRequest(samFunction, "createInstanceResource");
    }

    @Override
    public void deleteInstanceResource(UUID instanceId) {
        VoidSamFunction samFunction = () -> samClientFactory.getResourcesApi().deleteResourceV2(RESOURCE_NAME_INSTANCE, instanceId.toString());
        executeSamRequest(samFunction, "deleteInstanceResource");
    }


}


