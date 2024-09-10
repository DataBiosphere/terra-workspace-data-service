package org.databiosphere.workspacedataservice.shared.model;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public record EvaluateExpressionsWithArrayRequest(
    String arrayExpression, Collection<NamedExpression> expressions, int offset, int pageSize) {
  public Map<String, String> expressionsMap() {
    return expressions.stream()
        .collect(Collectors.toMap(NamedExpression::name, NamedExpression::expression));
  }
}
