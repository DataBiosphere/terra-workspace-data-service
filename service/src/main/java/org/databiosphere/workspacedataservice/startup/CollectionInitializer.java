package org.databiosphere.workspacedataservice.startup;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@DataPlane
@Profile({"!local"})
public class CollectionInitializer implements ApplicationListener<ContextRefreshedEvent> {

  private final CollectionInitializerBean collectionInitializerBean;

  public CollectionInitializer(CollectionInitializerBean collectionInitializerBean) {
    this.collectionInitializerBean = collectionInitializerBean;
  }

  @Override
  public void onApplicationEvent(@NotNull ContextRefreshedEvent event) {
    collectionInitializerBean.initializeCollection();
  }
}
