package org.databiosphere.workspacedataservice.shared.model;

import java.util.Collection;

public record ExpressionEvaluationsForRecord(
    String recordId, Collection<ExpressionEvaluation> evaluations) {}
