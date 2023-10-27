package org.databiosphere.workspacedataservice;

import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@Profile({"!local"})
public class InstanceInitializer implements ApplicationListener<ContextRefreshedEvent> {

  private final InstanceInitializerBean instanceInitializerBean;

  public InstanceInitializer(InstanceInitializerBean instanceInitializerBean) {
    this.instanceInitializerBean = instanceInitializerBean;
  }

  @Override
  public void onApplicationEvent(@NotNull ContextRefreshedEvent event) {
    instanceInitializerBean.initializeInstance();
  }
}
