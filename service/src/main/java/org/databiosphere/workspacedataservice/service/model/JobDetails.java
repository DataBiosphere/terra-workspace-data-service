package org.databiosphere.workspacedataservice.service.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JobDetails(List<JobHistory> jobHistory) {
}
