package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * Generic representation of a job, no opinion on inputs and result for the job. See individual APIs for more guidance on expected input and result payloads. 
 */

@Schema(name = "GenericJob", description = "Generic representation of a job, no opinion on inputs and result for the job. See individual APIs for more guidance on expected input and result payloads. ")
@JsonTypeName("GenericJob")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2023-10-11T10:58:05.173956-04:00[America/New_York]")
public class GenericJobServerModel {

  private String jobId;

  private String jobType;

  /**
   * Gets or Sets status
   */
  public enum StatusEnum {
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

  private String created;

  private String updated;

  private String errorMessage;

  private Object input;

  private Object result;

  /**
   * Default constructor
   * @deprecated Use {@link GenericJobServerModel#GenericJobServerModel(String, String, StatusEnum, String, String)}
   */
  @Deprecated
  public GenericJobServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public GenericJobServerModel(String jobId, String jobType, StatusEnum status, String created, String updated) {
    this.jobId = jobId;
    this.jobType = jobType;
    this.status = status;
    this.created = created;
    this.updated = updated;
  }

  public GenericJobServerModel jobId(String jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * Get jobId
   * @return jobId
  */
  @NotNull 
  @Schema(name = "jobId", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("jobId")
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public GenericJobServerModel jobType(String jobType) {
    this.jobType = jobType;
    return this;
  }

  /**
   * Get jobType
   * @return jobType
  */
  @NotNull 
  @Schema(name = "jobType", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("jobType")
  public String getJobType() {
    return jobType;
  }

  public void setJobType(String jobType) {
    this.jobType = jobType;
  }

  public GenericJobServerModel status(StatusEnum status) {
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

  public GenericJobServerModel created(String created) {
    this.created = created;
    return this;
  }

  /**
   * Get created
   * @return created
  */
  @NotNull 
  @Schema(name = "created", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("created")
  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public GenericJobServerModel updated(String updated) {
    this.updated = updated;
    return this;
  }

  /**
   * Get updated
   * @return updated
  */
  @NotNull 
  @Schema(name = "updated", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("updated")
  public String getUpdated() {
    return updated;
  }

  public void setUpdated(String updated) {
    this.updated = updated;
  }

  public GenericJobServerModel errorMessage(String errorMessage) {
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

  public GenericJobServerModel input(Object input) {
    this.input = input;
    return this;
  }

  /**
   * Input arguments for this job
   * @return input
  */
  
  @Schema(name = "input", description = "Input arguments for this job", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("input")
  public Object getInput() {
    return input;
  }

  public void setInput(Object input) {
    this.input = input;
  }

  public GenericJobServerModel result(Object result) {
    this.result = result;
    return this;
  }

  /**
   * Result of this job
   * @return result
  */
  
  @Schema(name = "result", description = "Result of this job", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("result")
  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
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
    GenericJobServerModel genericJob = (GenericJobServerModel) o;
    return Objects.equals(this.jobId, genericJob.jobId) &&
        Objects.equals(this.jobType, genericJob.jobType) &&
        Objects.equals(this.status, genericJob.status) &&
        Objects.equals(this.created, genericJob.created) &&
        Objects.equals(this.updated, genericJob.updated) &&
        Objects.equals(this.errorMessage, genericJob.errorMessage) &&
        Objects.equals(this.input, genericJob.input) &&
        Objects.equals(this.result, genericJob.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, jobType, status, created, updated, errorMessage, input, result);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GenericJobServerModel {\n");
    sb.append("    jobId: ").append(toIndentedString(jobId)).append("\n");
    sb.append("    jobType: ").append(toIndentedString(jobType)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    created: ").append(toIndentedString(created)).append("\n");
    sb.append("    updated: ").append(toIndentedString(updated)).append("\n");
    sb.append("    errorMessage: ").append(toIndentedString(errorMessage)).append("\n");
    sb.append("    input: ").append(toIndentedString(input)).append("\n");
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

