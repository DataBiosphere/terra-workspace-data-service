package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RecordRequest(@JsonProperty("attributes") RecordAttributes recordAttributes) {
}
