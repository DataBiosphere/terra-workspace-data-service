package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "type", "attributes"})
public record RecordResponse(
    @JsonProperty("id") String recordId,
    @JsonProperty("type") RecordType recordType,
    @JsonProperty("attributes") RecordAttributes recordAttributes) {}
