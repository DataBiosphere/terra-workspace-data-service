package org.databiosphere.workspacedataservice.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.web.bind.annotation.RestController;

/*A wrapper for our @RestControllers that requires us to specify whether we're exposing our APIs for
 * the control-plane, data-plane, or both.*/
public interface DeploymentRestController {

  @Target({ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @RestController
  @Conditional(DeploymentModeCondition.class)
  public @interface WdsRestController {
    String[] deploymentModes();
  }
}

class DeploymentModeCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    // get the deploymentModes value from the WdsRestController annotation
    String[] deploymentModes =
        (String[])
            metadata
                .getAnnotationAttributes(DeploymentRestController.WdsRestController.class.getName())
                .get("deploymentModes");

    // get the actual deployment mode from the environment
    String actualDeploymentMode = context.getEnvironment().getProperty("env.wds.deploymentMode");

    // check if the actual deployment mode matches any of the required deployment modes
    for (String deploymentMode : deploymentModes) {
      if (deploymentMode.equals(actualDeploymentMode)) {
        return true;
      }
    }
    return false;
  }
}
