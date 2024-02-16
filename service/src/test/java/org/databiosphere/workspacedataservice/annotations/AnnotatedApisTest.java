package org.databiosphere.workspacedataservice.annotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.web.bind.annotation.RestController;

@SpringBootTest
class AnnotatedApisTest {

  @Test
  void controllersMustHaveDeploymentAnnotation() {
    // scanner that detects classes annotated with @RestController
    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false);
    scanner.addIncludeFilter(new AnnotationTypeFilter(RestController.class));
    Set<BeanDefinition> restControllerComponents =
        scanner.findCandidateComponents("org.databiosphere.workspacedataservice.controller");

    for (BeanDefinition beanDefinition : restControllerComponents) {
      // extract the class from the bean, then check its annotations for either a DataPlane or
      // ControlPlane annotation
      Class<?> cls = assertDoesNotThrow(() -> Class.forName(beanDefinition.getBeanClassName()));
      assertTrue(
          Arrays.stream(cls.getAnnotations())
              .anyMatch(
                  n ->
                      n.annotationType().equals(DeploymentMode.DataPlane.class)
                          || n.annotationType().equals(DeploymentMode.ControlPlane.class)));
    }
  }
}
