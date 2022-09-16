package org.databiosphere.workspacedataservice.service.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AttributeSchema(String name, String datatype, RecordType relatesTo) {
}
