package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import java.util.Map;

public record EvaluateExpressionsResponse(Collection<ExpressionEvaluation> evaluations) {
  public static EvaluateExpressionsResponse of(Map<String, JsonNode> evaluations) {
    return new EvaluateExpressionsResponse(
        evaluations.entrySet().stream()
            .map(e -> new ExpressionEvaluation(e.getKey(), e.getValue()))
            .toList());
  }
}
