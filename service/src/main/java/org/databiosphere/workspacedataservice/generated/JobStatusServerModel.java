package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * JobStatusServerModel
 */

@JsonTypeName("JobStatus")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2023-10-05T16:58:02.946728-04:00[America/New_York]")
public class JobStatusServerModel {

  private UUID jobId;

  /**
   * Gets or Sets status
   */
  public enum StatusEnum {
    CREATED("CREATED"),
    
    QUEUED("QUEUED"),
    
    RUNNING("RUNNING"),
    
    SUCCEEDED("SUCCEEDED"),
    
    ERROR("ERROR"),
    
    CANCELLED("CANCELLED"),
    
    UNKNOWN("UNKNOWN");

    private String value;

    StatusEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static StatusEnum fromValue(String value) {
      for (StatusEnum b : StatusEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private StatusEnum status;

  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private OffsetDateTime created;

  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private OffsetDateTime updated;

  private String errorMessage;

  @Valid
  private Map<String, Object> result = new HashMap<>();

  /**
   * Default constructor
   * @deprecated Use {@link JobStatusServerModel#JobStatusServerModel(UUID, StatusEnum, OffsetDateTime, OffsetDateTime, Map<String, Object>)}
   */
  @Deprecated
  public JobStatusServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public JobStatusServerModel(UUID jobId, StatusEnum status, OffsetDateTime created, OffsetDateTime updated, Map<String, Object> result) {
    this.jobId = jobId;
    this.status = status;
    this.created = created;
    this.updated = updated;
    this.result = result;
  }

  public JobStatusServerModel jobId(UUID jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * Get jobId
   * @return jobId
  */
  @NotNull @Valid 
  @Schema(name = "jobId", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("jobId")
  public UUID getJobId() {
    return jobId;
  }

  public void setJobId(UUID jobId) {
    this.jobId = jobId;
  }

  public JobStatusServerModel status(StatusEnum status) {
    this.status = status;
    return this;
  }

  /**
   * Get status
   * @return status
  */
  @NotNull 
  @Schema(name = "status", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("status")
  public StatusEnum getStatus() {
    return status;
  }

  public void setStatus(StatusEnum status) {
    this.status = status;
  }

  public JobStatusServerModel created(OffsetDateTime created) {
    this.created = created;
    return this;
  }

  /**
   * Get created
   * @return created
  */
  @NotNull @Valid 
  @Schema(name = "created", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("created")
  public OffsetDateTime getCreated() {
    return created;
  }

  public void setCreated(OffsetDateTime created) {
    this.created = created;
  }

  public JobStatusServerModel updated(OffsetDateTime updated) {
    this.updated = updated;
    return this;
  }

  /**
   * Get updated
   * @return updated
  */
  @NotNull @Valid 
  @Schema(name = "updated", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("updated")
  public OffsetDateTime getUpdated() {
    return updated;
  }

  public void setUpdated(OffsetDateTime updated) {
    this.updated = updated;
  }

  public JobStatusServerModel errorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    return this;
  }

  /**
   * Get errorMessage
   * @return errorMessage
  */
  
  @Schema(name = "errorMessage", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("errorMessage")
  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public JobStatusServerModel result(Map<String, Object> result) {
    this.result = result;
    return this;
  }

  public JobStatusServerModel putResultItem(String key, Object resultItem) {
    if (this.result == null) {
      this.result = new HashMap<>();
    }
    this.result.put(key, resultItem);
    return this;
  }

  /**
   * Get result
   * @return result
  */
  @NotNull 
  @Schema(name = "result", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("result")
  public Map<String, Object> getResult() {
    return result;
  }

  public void setResult(Map<String, Object> result) {
    this.result = result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobStatusServerModel jobStatus = (JobStatusServerModel) o;
    return Objects.equals(this.jobId, jobStatus.jobId) &&
        Objects.equals(this.status, jobStatus.status) &&
        Objects.equals(this.created, jobStatus.created) &&
        Objects.equals(this.updated, jobStatus.updated) &&
        Objects.equals(this.errorMessage, jobStatus.errorMessage) &&
        Objects.equals(this.result, jobStatus.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, status, created, updated, errorMessage, result);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class JobStatusServerModel {\n");
    sb.append("    jobId: ").append(toIndentedString(jobId)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    created: ").append(toIndentedString(created)).append("\n");
    sb.append("    updated: ").append(toIndentedString(updated)).append("\n");
    sb.append("    errorMessage: ").append(toIndentedString(errorMessage)).append("\n");
    sb.append("    result: ").append(toIndentedString(result)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

