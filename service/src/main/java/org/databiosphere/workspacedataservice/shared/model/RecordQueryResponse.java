package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;

public record RecordQueryResponse(SearchRequest searchRequest, List<RecordResponse> records, int totalRecords) {
}
