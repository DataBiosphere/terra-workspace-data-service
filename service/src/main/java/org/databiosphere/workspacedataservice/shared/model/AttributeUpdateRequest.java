package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AttributeUpdateRequest(@JsonProperty("name") String name) {}
