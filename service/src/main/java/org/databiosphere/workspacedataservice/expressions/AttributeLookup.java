package org.databiosphere.workspacedataservice.expressions;

import java.util.List;

public record AttributeLookup(List<String> relations, String attribute, String lookupText) {}
