package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.pubsub.PubSubMessage;
import org.databiosphere.workspacedataservice.pubsub.PubSubRequest;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@ControlPlane
@RestController
public class PubSubController {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubController.class);
  private final JobService jobService;
  private final ObjectMapper objectMapper;

  public PubSubController(JobService jobService, ObjectMapper objectMapper) {
    this.jobService = jobService;
    this.objectMapper = objectMapper;
  }

  @PostMapping("/pubsub/import-status")
  public ResponseEntity<String> receiveImportNotification(@RequestBody PubSubRequest request) {
    PubSubMessage message = request.message();
    LOGGER.info(
        "Received PubSub message: {}, published {}", message.messageId(), message.publishTime());
    try {
      JobStatusUpdate update = JobStatusUpdate.createFromPubSubMessage(request.message());
      LOGGER.info(
          "Received status update for job {}: {} -> {}",
          update.jobId(),
          update.currentStatus(),
          update.newStatus());
      jobService.processStatusUpdate(update);
      return new ResponseEntity<>(HttpStatus.OK);
    } catch (ValidationException e) {
      // Return a successful status for invalid updates to prevent PubSub from retrying the request.
      // https://cloud.google.com/pubsub/docs/push#receive_push
      try {
        String jsonMessage = objectMapper.writeValueAsString(message);
        LOGGER.error("Error processing status update: %s".formatted(jsonMessage), e);
      } catch (JsonProcessingException jsonException) {
        LOGGER.error("Error processing status update", e);
      }
      return new ResponseEntity<>(HttpStatus.ACCEPTED);
    }
  }
}
