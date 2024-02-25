package org.databiosphere.workspacedataservice.controller;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@RestController
public class SwaggerUiController {

  @Autowired private Environment environment;

  @Value("classpath:swagger-ui-template.html")
  Resource swaggerUiResource;

  // redirect "/", "/swagger" and "/swagger/" to "/swagger/swagger-ui.html"
  @GetMapping({"/", "/swagger", "/swagger/"})
  public ModelAndView redirectWithUsingRedirectPrefix(ModelMap model) {
    return new ModelAndView("redirect:/swagger/swagger-ui.html", model);
  }

  @GetMapping("/swagger/swagger-ui.html")
  public ResponseEntity<String> getSwaggerUi() throws IOException {
    String template = new String(Files.readAllBytes(swaggerUiResource.getFile().toPath()));

    String content = "error";

    if (environment.matchesProfiles("control-plane")) {
      // replace the title
      content =
          template.replace("${TITLE}", "cWDS API").replace("${APISPEC}", "cwds-api-docs.yaml");
    } else {
      content =
          template
              .replace("${TITLE}", "Azure Data Plane WDS API")
              .replace("${APISPEC}", "openapi-docs.yaml");
    }

    return ResponseEntity.of(Optional.of(content));
  }
}
