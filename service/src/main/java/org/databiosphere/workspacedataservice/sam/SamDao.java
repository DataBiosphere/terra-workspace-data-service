package org.databiosphere.workspacedataservice.sam;

import java.util.UUID;

/**
 * Interface for SamDao, allowing various dao implementations.
 * Currently, the only implementation is HttpSamDao.
 */
public interface SamDao {

    /**
     * Sam resource type name for WDS instances
     */
    String RESOURCE_NAME_INSTANCE = "wds-instance";
    /**
     * Sam resource type name for Workspaces
     */
    String RESOURCE_NAME_WORKSPACE = "workspace";

    /**
     * Sam action name for write permission
     */
    String ACTION_WRITE = "write";
    /**
     * Sam action name for delete permission
     */
    String ACTION_DELETE = "delete";

    /**
     * Check if the current user has permission to create a "wds-instance" resource in Sam
     * @param parentWorkspaceId the workspaceId which will be the parent of the "wds-instance" resource
     * @return true if the user has permission
     */
    boolean hasCreateInstancePermission(UUID parentWorkspaceId);

    /**
     * Check if the current user has permission to delete a "wds-instance" resource from Sam
     * @param instanceId the id of the "wds-instance" resource to be deleted
     * @return true if the user has permission
     */
    boolean hasDeleteInstancePermission(UUID instanceId);

    /**
     * Creates a "wds-instance" Sam resource
     * @param instanceId the id to use for the "wds-instance" resource
     * @param parentWorkspaceId the id to use for the "wds-instance" resource's parent
     */
    void createInstanceResource(UUID instanceId, UUID parentWorkspaceId);

    /**
     * Deletes a "wds-instance" Sam resource
     * @param instanceId the id of the "wds-instance" resource to be deleted
     */
    void deleteInstanceResource(UUID instanceId);

    /**
     * Checks whether a "wds-instance" Sam resource already exists
     * @param instanceId wds-instance resource id
     * @return true if wds-resource with this id already exists in Sam
     */
    boolean instanceResourceExists(UUID instanceId);

}
