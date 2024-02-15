package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import org.databiosphere.workspacedataservice.annotations.DeploymentRestController.WdsRestController;
import org.databiosphere.workspacedataservice.generated.CapabilitiesApi;
import org.databiosphere.workspacedataservice.generated.CapabilitiesServerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Controller for capabilities-related APIs */
@WdsRestController(deploymentModes = {"data-plane"})
public class CapabilitiesController implements CapabilitiesApi {

  private final CapabilitiesServerModel capabilitiesServerModel;

  public CapabilitiesController(ObjectMapper objectMapper) {
    // read the "capabilities.json" file from resources, and parse it into a CapabilitiesServerModel
    InputStream inputStream = getClass().getResourceAsStream("/capabilities.json");
    CapabilitiesServerModel tempModel;
    Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    try {
      tempModel = objectMapper.readValue(inputStream, CapabilitiesServerModel.class);
      logger.info("Capabilities initialized: {}", tempModel.entrySet());
    } catch (IOException e) {
      logger.error("Could not initialize capabilities: " + e.getMessage(), e);
      tempModel = new CapabilitiesServerModel();
    }
    capabilitiesServerModel = tempModel;
  }

  @Override
  @JsonPropertyOrder(alphabetic = true)
  public ResponseEntity<CapabilitiesServerModel> capabilities() {
    return new ResponseEntity<>(capabilitiesServerModel, HttpStatus.OK);
  }
}
