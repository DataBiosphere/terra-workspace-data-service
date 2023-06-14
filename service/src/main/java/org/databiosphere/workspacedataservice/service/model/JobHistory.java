package org.databiosphere.workspacedataservice.service.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JobHistory(String state, String exceptionType, String exceptionMessage) {
}
