package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.databind.JsonNode;

public record ExpressionEvaluation(String name, JsonNode value) {}
