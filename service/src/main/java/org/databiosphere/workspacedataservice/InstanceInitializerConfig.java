package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.distributed.DistributedLock;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstanceInitializerConfig {

  @Bean
  public InstanceInitializerBean instanceInitializerBean(
      InstanceDao instanceDao,
      LeonardoDao leoDao,
      WorkspaceDataServiceDao wdsDao,
      CloneDao cloneDao,
      BackupRestoreService restoreService,
      DistributedLock lock) {
    return new InstanceInitializerBean(instanceDao, leoDao, wdsDao, cloneDao, restoreService, lock);
  }
}
