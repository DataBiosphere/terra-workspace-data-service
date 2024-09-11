package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

public record ExpressionEvaluationsForRecord(String recordId, Map<String, JsonNode> evaluations) {}
