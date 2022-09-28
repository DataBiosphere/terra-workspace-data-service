package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public record BatchResponse(int recordsModified, String success) {

    @JsonCreator
    public BatchResponse {
    }
}
