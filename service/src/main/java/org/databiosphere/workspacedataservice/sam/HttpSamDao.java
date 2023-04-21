package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;

import static org.databiosphere.workspacedataservice.sam.HttpSamClientSupport.SamFunction;

/**
 * Implementation of SamDao that accepts a SamClientFactory,
 * then asks that factory for a new ResourcesApi to use within each
 * method invocation.
 */
public class HttpSamDao implements SamDao {

    protected final SamClientFactory samClientFactory;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamDao.class);
    private final HttpSamClientSupport httpSamClientSupport;
    private final String workspaceId;

    public HttpSamDao(SamClientFactory samClientFactory, HttpSamClientSupport httpSamClientSupport, String workspaceId) {
        this.samClientFactory = samClientFactory;
        this.httpSamClientSupport = httpSamClientSupport;
        this.workspaceId = workspaceId;
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
        return hasCreateInstancePermission(parentWorkspaceId, null);
    }

    @Override
    public boolean hasCreateInstancePermission(UUID parentWorkspaceId, String token) {
        return hasPermission(RESOURCE_NAME_WORKSPACE, parentWorkspaceId.toString(), ACTION_WRITE,
                "hasCreateInstancePermission", token);
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
        return hasDeleteInstancePermission(instanceId, null);
    }
    @Override
    public boolean hasDeleteInstancePermission(UUID instanceId, String token) {
        return hasPermission(RESOURCE_NAME_WORKSPACE, instanceId.toString(), ACTION_DELETE,
                "hasDeleteInstancePermission", token);
    }

    // helper implementation for permission checks
    private boolean hasPermission(String resourceType, String resourceId, String action, String loggerHint, String token) {
        LOGGER.debug("Checking Sam permission for {}/{}/{} on behalf of instance {} ...", resourceType, workspaceId, action, resourceId);
        SamFunction<Boolean> samFunction = () -> samClientFactory.getResourcesApi(token)
                .resourcePermissionV2(resourceType, workspaceId, action);
        return httpSamClientSupport.withRetryAndErrorHandling(samFunction, loggerHint);
    }

    /**
     * Check if the current user has permission to write to a "wds-instance" resource from Sam.
     * Implemented as a check for write permission on the resource.
     *
     * @param instanceId the id of the "wds-instance" resource to be written to
     * @return true if the user has permission
     */
    @Override
    public boolean hasWriteInstancePermission(UUID instanceId) {
        return hasWriteInstancePermission(instanceId, null);
    }

    @Override
    public boolean hasWriteInstancePermission(UUID instanceId, String token) {
        return hasPermission(RESOURCE_NAME_WORKSPACE, instanceId.toString(), ACTION_WRITE,
                "hasWriteInstancePermission", token);
    }

    /**
     * Gets the System Status of Sam. Using @Cacheable, will reach out to Sam no more than once every 5 minutes.
     * See also emptySamStatusCache()
     */
    @Cacheable(value = "samStatus", key="'getSystemStatus'")
    public SystemStatus getSystemStatus() {
        SamFunction<SystemStatus> samFunction = () -> samClientFactory.getStatusApi().getSystemStatus();
        return httpSamClientSupport.withRetryAndErrorHandling(samFunction, "getSystemStatus");
    }

    /**
     * Clears the samStatus cache every 5 minutes, to ensure we get fresh results from Sam
     * every so often. See also getSystemStatus()
     */

    @CacheEvict(value = "samStatus", key="'getSystemStatus'")
    @Scheduled(fixedRateString = "${sam.healthcheck.pingTTL}")
    public void emptySamStatusCache() {
        LOGGER.debug("emptying samStatus cache");
    }
}


