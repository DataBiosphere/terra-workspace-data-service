package org.databiosphere.workspacedataservice.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;

public record JobStatusUpdate(
    UUID jobId, StatusEnum currentStatus, StatusEnum newStatus, @Nullable String errorMessage) {
  public JobStatusUpdate(UUID jobId, StatusEnum currentStatus, StatusEnum newStatus) {
    this(jobId, currentStatus, newStatus, null);
  }

  public static JobStatusUpdate createFromPubSubMessage(PubsubMessage message) {
    try {
      Map<String, String> attributes = message.getAttributesMap();
      UUID jobId = UUID.fromString(attributes.get("import_id"));
      StatusEnum newStatus = rawlsStatusToJobStatus(attributes.get("new_status"));
      StatusEnum currentStatus = rawlsStatusToJobStatus(attributes.get("current_status"));
      String errorMessage = attributes.get("error_message");
      return new JobStatusUpdate(jobId, currentStatus, newStatus, errorMessage);
    } catch (Exception e) {
      throw new ValidationException(
          "Unable to parse job status update from PubSub message: %s".formatted(e.getMessage()), e);
    }
  }

  private static StatusEnum rawlsStatusToJobStatus(String rawlsStatus) {
    return switch (rawlsStatus) {
      case "ReadyForUpsert" -> StatusEnum.RUNNING;
      case "Upserting" -> StatusEnum.RUNNING;
      case "Done" -> StatusEnum.SUCCEEDED;
      case "Error" -> StatusEnum.ERROR;
      default -> throw new IllegalArgumentException(
          "Unknown Rawls import status: %s".formatted(rawlsStatus));
    };
  }
}
