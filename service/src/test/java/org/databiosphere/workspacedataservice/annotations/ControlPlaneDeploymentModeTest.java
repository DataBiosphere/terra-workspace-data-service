package org.databiosphere.workspacedataservice.annotations;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(
    profiles = {"control-plane"},
    inheritProfiles = false)
class ControlPlaneDeploymentModeTest extends DeploymentModeTestBase {

  @ParameterizedTest(name = "{0} is disabled for DeploymentMode control-plane")
  @MethodSource("dataPlaneConditionalBeans")
  void beansDisabledForControlPlane(String beanName) {
    // bean should not exist in this context if not tagged with @ControlPlane
    assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(beanName));
  }

  @ParameterizedTest(name = "{0} is enabled for DeploymentMode control-plane only")
  @MethodSource("controlPlaneConditionalBeans")
  void beansEnabledForControlPlaneOnly(String beanName) {
    assertNotNull(context.findAnnotationOnBean(beanName, ControlPlane.class));
  }
}
