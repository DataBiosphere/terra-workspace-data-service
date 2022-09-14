package org.databiosphere.workspacedataservice.service.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AttributeSchema(String name, String datatype, String relatesTo) {
}
