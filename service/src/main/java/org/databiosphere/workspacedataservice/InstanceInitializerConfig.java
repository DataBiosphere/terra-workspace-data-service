package org.databiosphere.workspacedataservice;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;

import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.locks.LockRegistry;

@Configuration
public class InstanceInitializerConfig {

  @DataPlane
  @Bean
  public InstanceInitializerBean instanceInitializerBean(
      InstanceDao instanceDao,
      LeonardoDao leoDao,
      WorkspaceDataServiceDao wdsDao,
      CloneDao cloneDao,
      BackupRestoreService restoreService,
      LockRegistry lockRegistry) {
    return new InstanceInitializerBean(
        instanceDao, leoDao, wdsDao, cloneDao, restoreService, lockRegistry);
  }
}
