package org.databiosphere.workspacedataservice.shared.model;


public record SearchRequest(int pageSize, int offset, SortDirection sortDirection) {
}
