package org.databiosphere.workspacedataservice.pubsub;

public record PubSubRequest(PubSubMessage message, Integer deliveryAttempt, String subscription) {}
