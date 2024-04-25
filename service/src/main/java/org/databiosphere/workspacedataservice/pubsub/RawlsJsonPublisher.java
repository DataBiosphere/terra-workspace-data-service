package org.databiosphere.workspacedataservice.pubsub;

import static java.util.Objects.requireNonNull;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Publishes a message to pub/sub for the given {@link ImportDetails}. */
public class RawlsJsonPublisher {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final PubSub pubSub;
  private final Blob blob;
  private final ImportDetails importDetails;
  private final boolean isUpsert;

  public RawlsJsonPublisher(
      PubSub pubSub, ImportDetails importDetails, Blob blob, boolean isUpsert) {
    this.pubSub = pubSub;
    this.blob = blob;
    this.importDetails = importDetails;
    this.isUpsert = isUpsert;
  }

  public void publish() {
    UUID jobId = requireNonNull(importDetails.jobId());
    String user =
        requireNonNull(
                importDetails.userEmailSupplier(),
                "Expected ImportDetails.userEmailSupplier to be non-null for async imports")
            .get();
    Map<String, String> message =
        new ImmutableMap.Builder<String, String>()
            .put("workspaceId", importDetails.workspaceId().toString())
            .put("userEmail", user)
            .put("jobId", jobId.toString())
            .put("upsertFile", blob.getBucket() + "/" + blob.getName())
            .put("isUpsert", String.valueOf(isUpsert))
            .put("isCWDS", "true")
            .build();
    logger.info("Publishing message to pub/sub for job {} ...", jobId);
    String publishResult = pubSub.publishSync(message);
    logger.info("Pub/sub publishing complete for job {}: {}", jobId, publishResult);
  }
}
