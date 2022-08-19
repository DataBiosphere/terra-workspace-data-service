package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

public record RecordRequest(@JsonProperty("attributes") RecordAttributes recordAttributes) {
}
