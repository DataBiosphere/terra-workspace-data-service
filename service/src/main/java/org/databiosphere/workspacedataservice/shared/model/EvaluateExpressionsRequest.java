package org.databiosphere.workspacedataservice.shared.model;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public record EvaluateExpressionsRequest(Collection<NamedExpression> expressions) {
  public Map<String, String> toMap() {
    return expressions.stream()
        .collect(Collectors.toMap(NamedExpression::name, NamedExpression::expression));
  }
}
