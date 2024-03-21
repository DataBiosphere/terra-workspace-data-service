package org.databiosphere.workspacedataservice.pubsub;

import java.util.Map;

public record PubSubMessage(
    Map<String, String> attributes, String data, String messageId, String publishTime) {}
