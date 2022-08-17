package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "type", "attributes", "metadata"})
public record RecordResponse(@JsonProperty("id") RecordId recordId, @JsonProperty("type") RecordType recordType,
		@JsonProperty("attributes") RecordAttributes recordAttributes,
		@JsonProperty("metadata") RecordMetadata recordMetadata) {
}
