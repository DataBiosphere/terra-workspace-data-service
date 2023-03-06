package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;


public class HttpSamDao implements SamDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSamDao.class);

    private final SamClientFactory samClientFactory;

    public HttpSamDao(SamClientFactory samClientFactory) {
        this.samClientFactory = samClientFactory;

    }

    @Override
    public void createInstanceResource(UUID instanceId, UUID parentWorkspaceId) {
        // TODO:
        // - retries
        // - more detail in error logging
        // - handle 409s?
        // - extract CreateResourceRequestV2 object construction to helper method
        if (hasCreateInstancePermission(parentWorkspaceId)) {
            FullyQualifiedResourceId parent = new FullyQualifiedResourceId();
            parent.setResourceTypeName(RESOURCE_NAME_WORKSPACE);
            parent.setResourceId(parentWorkspaceId.toString());

            CreateResourceRequestV2 createResourceRequest = new CreateResourceRequestV2();
            createResourceRequest.setResourceId(instanceId.toString());
            createResourceRequest.setParent(parent);
            createResourceRequest.setAuthDomain(Collections.emptyList());

            try {
                LOGGER.info("sending createInstanceResource request to Sam ...");
//                samClientFactory.getResourcesApi()
//                        .createResourceV2(RESOURCE_NAME_INSTANCE, createResourceRequest);
                LOGGER.info("createInstanceResource Sam request succeeded.");
//            } catch (org.broadinstitute.dsde.workbench.client.sam.ApiException apiException) {
//                LOGGER.error("createInstanceResource Sam request resulted in: {} {}",
//                        apiException.getCode(), apiException.getResponseBody());
            } catch (Exception e) {
                LOGGER.error("createInstanceResource Sam request resulted in: {}", e.getMessage());
            }
        }
    }

    @Override
    public void deleteInstanceResource(UUID instanceId) {
        try {
            LOGGER.info("sending deleteInstanceResource request to Sam ...");
//            samClientFactory.getResourcesApi().deleteResourceV2(RESOURCE_NAME_INSTANCE, instanceId.toString());
            LOGGER.info("deleteInstanceResource Sam request succeeded.");
//        } catch (org.broadinstitute.dsde.workbench.client.sam.ApiException apiException) {
//            LOGGER.error("deleteInstanceResource Sam request resulted in: {} {}",
//                    apiException.getCode(), apiException.getResponseBody());
        } catch (Exception e) {
            LOGGER.error("deleteInstanceResource Sam request resulted in: {}", e.getMessage());
        }
    }

    @Override
    public boolean hasCreateInstancePermission(UUID parentWorkspaceId) {
        // TODO:
        // - retries
        // - error logging
        // -
        try {
            LOGGER.info("sending hasCreateInstancePermission request to Sam ...");
            return samClientFactory.getResourcesApi()
                    .resourcePermissionV2(RESOURCE_NAME_WORKSPACE, parentWorkspaceId.toString(), ACTION_WRITE);
        } catch (org.broadinstitute.dsde.workbench.client.sam.ApiException apiException) {
            LOGGER.error("hasCreateInstancePermission Sam request resulted in: {} {}",
                    apiException.getCode(), apiException.getResponseBody());
            return false;
        } catch (Exception e) {
            LOGGER.error("hasCreateInstancePermission Sam request resulted in: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean hasDeleteInstancePermission(UUID instanceId) {
        // TODO:
        // - retries
        // - error logging
        // -
        try {
            LOGGER.info("sending hasDeleteInstancePermission request to Sam ...");
            return samClientFactory.getResourcesApi()
                    .resourcePermissionV2(RESOURCE_NAME_INSTANCE, instanceId.toString(), ACTION_DELETE);
        } catch (org.broadinstitute.dsde.workbench.client.sam.ApiException apiException) {
            LOGGER.error("hasDeleteInstancePermission Sam request resulted in: {} {}",
                    apiException.getCode(), apiException.getResponseBody());
            return false;
        } catch (Exception e) {
            LOGGER.error("hasDeleteInstancePermission Sam request resulted in: {}", e.getMessage());
            throw e;
        }
    }
}
