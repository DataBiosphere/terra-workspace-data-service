package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import java.util.Map;

public record EvaluateExpressionsWithArrayResponse(
    Collection<ExpressionEvaluationsForRecord> results) {
  public static EvaluateExpressionsWithArrayResponse of(
      Map<String, Map<String, JsonNode>> evaluations) {
    return new EvaluateExpressionsWithArrayResponse(
        evaluations.entrySet().stream()
            .map(
                recordIdAndResults ->
                    new ExpressionEvaluationsForRecord(
                        recordIdAndResults.getKey(),
                        recordIdAndResults.getValue().entrySet().stream()
                            .map(
                                expressionNameAndValue ->
                                    new ExpressionEvaluation(
                                        expressionNameAndValue.getKey(),
                                        expressionNameAndValue.getValue()))
                            .toList()))
            .toList());
  }
}
