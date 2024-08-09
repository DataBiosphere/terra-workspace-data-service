package org.databiosphere.workspacedataservice.search;

import java.util.List;
import java.util.Map;

public record WhereClausePart(List<String> clauses, Map<String, ?> values) {}
