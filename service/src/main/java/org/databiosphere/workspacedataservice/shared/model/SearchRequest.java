package org.databiosphere.workspacedataservice.shared.model;

public record SearchRequest(int limit, int offset, SortDirection sort) {
}
