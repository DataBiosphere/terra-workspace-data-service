package org.databiosphere.workspacedataservice.service;

import com.google.pubsub.v1.PubsubMessage;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PubSubService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubService.class);
  private final JobService jobService;

  public PubSubService(JobService jobService) {
    this.jobService = jobService;
  }

  public void processPubSubMessage(PubsubMessage message) {
    LOGGER.info(
        "Received PubSub message: {} {}",
        message.getMessageId(),
        message.getAttributesMap().entrySet().stream()
            .map(e -> "%s: %s".formatted(e.getKey(), e.getValue()))
            .collect(Collectors.joining(", ")));

    try {
      JobStatusUpdate update = JobStatusUpdate.createFromPubSubMessage(message);
      LOGGER.info(
          "Processing status update for job {}: {} -> {}",
          update.jobId(),
          update.currentStatus(),
          update.newStatus());

      try {
        jobService.processStatusUpdate(update);
      } catch (MissingObjectException e) {
        // Via PubSub, CWDS will receive status updates for both CWDS and import service jobs.
        // If the job is not found in the database (because it's an import service job), ignore it.
        LOGGER.info("Received status update for unknown job {}", update.jobId());
      }
    } catch (ValidationException e) {
      // Ignore messages that aren't valid status updates to prevent PubSub from retrying the
      // message.
    }
  }
}
