package org.databiosphere.workspacedataservice.pubsub;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record PubSubMessage(
    @JsonProperty("attributes") Map<String, String> attributes,
    @JsonProperty("data") String data,
    @JsonProperty("messageId") String messageId,
    @JsonProperty("publishTime") String publishTime) {}
