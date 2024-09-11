package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

public record EvaluateExpressionsResponse(Map<String, JsonNode> evaluations) {}
