package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AttributeSchemaUpdate(@JsonProperty("name") String name) {}
