package org.databiosphere.workspacedataservice.annotations;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"data-plane"})
class DataPlaneDeploymentModeTest extends DeploymentModeTestBase {

  @ParameterizedTest(name = "{0} is enabled for DeploymentMode data-plane only")
  @MethodSource("dataPlaneConditionalBeans")
  void beansEnabledForDataPlaneOnly(String beanName) {
    assertNotNull(context.findAnnotationOnBean(beanName, DataPlane.class));
  }

  /* TODO: Enable when we have parameters
   @ParameterizedTest(name = "{0} is disabled for DeploymentMode data-plane")
   @MethodSource("controlPlaneConditionalBeans")
   void beansDisabledForDataPlane(String beanName) {
       assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(beanName));
   }
  */
}
