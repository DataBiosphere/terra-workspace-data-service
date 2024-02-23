package org.databiosphere.workspacedataservice.startup;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.config.InstanceProperties;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@DataPlane
public class CollectionInitializer implements ApplicationListener<ContextRefreshedEvent> {

  private final CollectionInitializerBean collectionInitializerBean;
  private final boolean runOnStartup;

  public CollectionInitializer(
      CollectionInitializerBean collectionInitializerBean, InstanceProperties instanceProperties) {
    this.collectionInitializerBean = collectionInitializerBean;
    this.runOnStartup = instanceProperties.getInitializeCollectionOnStartup();
  }

  @Override
  public void onApplicationEvent(@NotNull ContextRefreshedEvent event) {
    if (runOnStartup) {
      collectionInitializerBean.initializeCollection();
    }
  }
}
