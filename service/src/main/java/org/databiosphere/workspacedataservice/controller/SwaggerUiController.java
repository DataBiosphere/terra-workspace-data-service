package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@ControlPlane
@DataPlane
@RestController
public class SwaggerUiController {
  /**
   * holds the HTML content of the swagger-ui.
   *
   * @see SwaggerUiConfig
   */
  final String swaggerUiContent;

  public SwaggerUiController(@Qualifier("swaggerHtml") String swaggerUiContent) {
    this.swaggerUiContent = swaggerUiContent;
  }

  // redirect "/", "/swagger" and "/swagger/" to "/swagger/swagger-ui.html"
  @GetMapping({"/", "/swagger", "/swagger/"})
  public ModelAndView redirectWithUsingRedirectPrefix(ModelMap model) {
    return new ModelAndView("redirect:/swagger/swagger-ui.html", model);
  }

  // serve up the swagger-ui html
  @GetMapping(value = "/swagger/swagger-ui.html", produces = MediaType.TEXT_HTML_VALUE)
  public @ResponseBody String getSwaggerUi() {
    return swaggerUiContent;
  }
}
