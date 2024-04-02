package org.databiosphere.workspacedataservice.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.junit.jupiter.api.Test;

class JobStatusUpdateTest {
  @Test
  void createFromPubSubMessageSuccess() {
    // Arrange
    UUID importId = UUID.randomUUID();
    PubsubMessage message =
        buildPubsubMessage(
            Map.of(
                "importId", importId.toString(),
                "currentStatus", "Upserting",
                "newStatus", "Done"));

    // Act
    JobStatusUpdate update = JobStatusUpdate.createFromPubSubMessage(message);

    // Assert
    assertEquals(importId, update.jobId());
    assertEquals(StatusEnum.RUNNING, update.currentStatus());
    assertEquals(StatusEnum.SUCCEEDED, update.newStatus());
  }

  @Test
  void createFromPubSubMessageError() {
    // Arrange
    UUID importId = UUID.randomUUID();
    PubsubMessage message =
        buildPubsubMessage(
            Map.of(
                "importId", importId.toString(),
                "currentStatus", "Upserting",
                "newStatus", "Error",
                "errorMessage", "Something went wrong"));

    // Act
    JobStatusUpdate update = JobStatusUpdate.createFromPubSubMessage(message);

    // Assert
    assertEquals(importId, update.jobId());
    assertEquals(StatusEnum.RUNNING, update.currentStatus());
    assertEquals(StatusEnum.ERROR, update.newStatus());
    assertEquals("Something went wrong", update.errorMessage());
  }

  @Test
  void createFromPubSubMessageNoCurrentStatus() {
    // Arrange
    UUID importId = UUID.randomUUID();
    PubsubMessage message =
        buildPubsubMessage(Map.of("importId", importId.toString(), "newStatus", "Error"));

    // Act
    JobStatusUpdate update = JobStatusUpdate.createFromPubSubMessage(message);

    // Assert
    assertEquals(importId, update.jobId());
    assertEquals(StatusEnum.UNKNOWN, update.currentStatus());
    assertEquals(StatusEnum.ERROR, update.newStatus());
  }

  private PubsubMessage buildPubsubMessage(Map<String, String> attributes) {
    return PubsubMessage.newBuilder()
        .setMessageId("testmessage")
        .putAllAttributes(attributes)
        .build();
  }
}
