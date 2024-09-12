package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import java.util.Map;

public record EvaluateExpressionsWithArrayResponse(
    Collection<ExpressionEvaluationsForRecord> results, boolean hasNext) {
  public static EvaluateExpressionsWithArrayResponse of(
      Map<String, Map<String, JsonNode>> evaluations, boolean hasNext) {
    return new EvaluateExpressionsWithArrayResponse(
        evaluations.entrySet().stream()
            .map(
                recordIdAndResults ->
                    new ExpressionEvaluationsForRecord(
                        recordIdAndResults.getKey(), recordIdAndResults.getValue()))
            .toList(),
        hasNext);
  }
}
