package org.databiosphere.workspacedataservice.sam;

import java.util.UUID;

/**
 * interface for SamDao, allowing various dao implementations. May not be necessary, if the
 * SamClientFactory interface flexibility allows everything we need.
 */
public interface SamDao {

    String RESOURCE_NAME_INSTANCE = "wds-instance";
    String RESOURCE_NAME_WORKSPACE = "workspace";
    String ACTION_WRITE = "write";
    String ACTION_DELETE = "delete";


    void createInstanceResource(UUID instanceId, UUID parentWorkspaceId);

    void deleteInstanceResource(UUID instanceId);

    boolean hasCreateInstancePermission(UUID parentWorkspaceId);

    boolean hasDeleteInstancePermission(UUID instanceId);

}
