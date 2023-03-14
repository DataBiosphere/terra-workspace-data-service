package org.databiosphere.workspacedataservice.sam;

import org.apache.logging.log4j.spi.AbstractLogger;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of SamDao that accepts a SamClientFactory,
 * then asks that factory for a new ResourcesApi to use within each
 * method invocation.
 */
public class HttpSamDao extends HttpSamClientSupport implements SamDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamDao.class);
    private final SamClientFactory samClientFactory;

    public HttpSamDao(SamClientFactory samClientFactory) {
        this.samClientFactory = samClientFactory;

    }

    /**
     * Check if the current user has permission to create a "wds-instance" resource in Sam.
     * Implemented as a check for write permission on the workspace which will contain this instance.
     *
     * @param parentWorkspaceId the workspaceId which will be the parent of the "wds-instance" resource
     * @return true if the user has permission
     */
    @Override
    public boolean hasCreateInstancePermission(UUID parentWorkspaceId) {
        return hasPermission(RESOURCE_NAME_WORKSPACE, parentWorkspaceId.toString(), ACTION_WRITE,
                "hasCreateInstancePermission");
    }

    /**
     * Check if the current user has permission to delete a "wds-instance" resource from Sam.
     * Implemented as a check for delete permission on the resource.
     *
     * @param instanceId the id of the "wds-instance" resource to be deleted
     * @return true if the user has permission
     */
    @Override
    public boolean hasDeleteInstancePermission(UUID instanceId) {
        return hasPermission(RESOURCE_NAME_INSTANCE, instanceId.toString(), ACTION_DELETE,
                "hasDeleteInstancePermission");
    }

    // helper implementation for permission checks
    private boolean hasPermission(String resourceType, String resourceId, String action, String loggerHint) {
        SamFunction<Boolean> samFunction = () -> samClientFactory.getResourcesApi()
                .resourcePermissionV2(resourceType, resourceId, action);
        return withSamErrorHandling(samFunction, loggerHint);
    }

    /**
     * Creates a "wds-instance" Sam resource.
     * Assigns the "wds-instance" resource to be a child of a workspace; within Sam's config
     * the "wds-instance" resource will inherit permissions from its parent workspace.
     *
     * @param instanceId the id to use for the "wds-instance" resource
     * @param parentWorkspaceId the id to use for the "wds-instance" resource's parent
     */
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
        withSamErrorHandling(samFunction, "createInstanceResource");
    }

    /**
     * Deletes a "wds-instance" Sam resource.
     *
     * @param instanceId the id of the "wds-instance" resource to be deleted
     */
    @Override
    public void deleteInstanceResource(UUID instanceId) {
        VoidSamFunction samFunction = () -> samClientFactory.getResourcesApi().deleteResourceV2(RESOURCE_NAME_INSTANCE, instanceId.toString());
        withSamErrorHandling(samFunction, "deleteInstanceResource");
    }

    /**
     * Gets the System Status of Sam. Using @Cacheable, will reach out to Sam no more than once every 5 minutes.
     * See also emptySamStatusCache()
     */
    @Cacheable(value = "samStatus")
    public SystemStatus getSystemStatus() {
        SamFunction<SystemStatus> samFunction = () -> samClientFactory.getStatusApi().getSystemStatus();
        return withSamErrorHandling(samFunction, "getSystemStatus");
    }

    /**
     * Clears the samStatus cache every 5 minutes, to ensure we get fresh results from Sam
     * every so often. See also getSystemStatus()
     */
    @CacheEvict(value = "samStatus", allEntries = true)
    @Scheduled(fixedRate = 5, timeUnit = TimeUnit.MINUTES)
    public void emptySamStatusCache() {
        LOGGER.debug("emptying samStatus cache");
    }

}


