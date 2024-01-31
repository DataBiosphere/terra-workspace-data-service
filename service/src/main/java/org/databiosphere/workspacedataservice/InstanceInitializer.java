package org.databiosphere.workspacedataservice;

import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@Profile({"!local"})
public class InstanceInitializer implements ApplicationListener<ContextRefreshedEvent> {

  private final Optional<InstanceInitializerBean> instanceInitializerBean;

  public InstanceInitializer(
      ObjectProvider<InstanceInitializerBean> instanceInitializerBeanProvider) {
    this.instanceInitializerBean =
        Optional.ofNullable(instanceInitializerBeanProvider.getIfAvailable());
  }

  @Override
  public void onApplicationEvent(@NotNull ContextRefreshedEvent event) {
    instanceInitializerBean.ifPresent(InstanceInitializerBean::initializeInstance);
  }
}
