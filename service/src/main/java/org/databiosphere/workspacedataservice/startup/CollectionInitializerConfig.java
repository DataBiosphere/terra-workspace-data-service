package org.databiosphere.workspacedataservice.startup;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.locks.LockRegistry;

@Configuration
public class CollectionInitializerConfig {

  @DataPlane
  @Bean
  public CollectionInitializerBean collectionInitializerBean(
      CollectionDao collectionDao,
      LeonardoDao leoDao,
      WorkspaceDataServiceDao wdsDao,
      CloneDao cloneDao,
      BackupRestoreService restoreService,
      LockRegistry lockRegistry) {
    return new CollectionInitializerBean(
        collectionDao, leoDao, wdsDao, cloneDao, restoreService, lockRegistry);
  }
}
