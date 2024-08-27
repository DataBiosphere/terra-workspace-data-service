package org.databiosphere.workspacedataservice.startup;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.locks.LockRegistry;

@Configuration
public class CollectionInitializerConfig {

  @DataPlane
  @Bean
  public CollectionInitializerBean collectionInitializerBean(
      CollectionService collectionService,
      LeonardoDao leoDao,
      WorkspaceDataServiceDao wdsDao,
      CloneDao cloneDao,
      BackupRestoreService restoreService,
      LockRegistry lockRegistry,
      @SingleTenant WorkspaceId workspaceId,
      @Value("${twds.instance.source-workspace-id}") String sourceWorkspaceIdString,
      @Value("${twds.startup-token}") String startupToken) {
    return new CollectionInitializerBean(
        collectionService,
        leoDao,
        wdsDao,
        cloneDao,
        restoreService,
        lockRegistry,
        workspaceId,
        sourceWorkspaceIdString,
        startupToken);
  }
}
