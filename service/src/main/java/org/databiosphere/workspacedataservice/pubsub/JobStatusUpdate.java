package org.databiosphere.workspacedataservice.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.springframework.lang.Nullable;

public record JobStatusUpdate(
    UUID jobId, StatusEnum currentStatus, StatusEnum newStatus, @Nullable String errorMessage) {
  public JobStatusUpdate(UUID jobId, StatusEnum currentStatus, StatusEnum newStatus) {
    this(jobId, currentStatus, newStatus, null);
  }

  public static JobStatusUpdate createFromPubSubMessage(PubsubMessage message) {
    try {
      Map<String, String> attributes = message.getAttributesMap();
      UUID jobId = UUID.fromString(attributes.get("importId"));
      StatusEnum newStatus = rawlsStatusToJobStatus(attributes.get("newStatus"));
      StatusEnum currentStatus = rawlsStatusToJobStatus(attributes.get("currentStatus"));
      String errorMessage = attributes.get("errorMessage");
      return new JobStatusUpdate(jobId, currentStatus, newStatus, errorMessage);
    } catch (Exception e) {
      throw new ValidationException(
          "Unable to parse job status update from PubSub message: %s".formatted(e.getMessage()), e);
    }
  }

  private static StatusEnum rawlsStatusToJobStatus(@Nullable String rawlsStatus) {
    if (rawlsStatus == null) {
      return StatusEnum.UNKNOWN;
    }
    return switch (rawlsStatus) {
      case "ReadyForUpsert" -> StatusEnum.RUNNING;
      case "Upserting" -> StatusEnum.RUNNING;
      case "Done" -> StatusEnum.SUCCEEDED;
      case "Error" -> StatusEnum.ERROR;
      default ->
          throw new IllegalArgumentException(
              "Unknown Rawls import status: %s".formatted(rawlsStatus));
    };
  }
}
