package org.databiosphere.workspacedataservice.pubsub;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PubSubRequest(
    @JsonProperty("message") PubSubMessage message,
    @JsonProperty("deliveryAttempt") Integer deliveryAttempt,
    @JsonProperty("subscription") String subscription) {}
