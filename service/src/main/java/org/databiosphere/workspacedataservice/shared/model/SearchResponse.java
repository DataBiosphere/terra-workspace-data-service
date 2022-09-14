package org.databiosphere.workspacedataservice.shared.model;


import java.util.List;

public record SearchResponse(SearchRequest searchRequest, List<Record> records) {
}
