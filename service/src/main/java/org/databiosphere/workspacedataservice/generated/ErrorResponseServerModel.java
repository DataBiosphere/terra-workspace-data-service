package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * 
 */

@Schema(name = "ErrorResponse", description = "")
@JsonTypeName("ErrorResponse")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class ErrorResponseServerModel {

  private Integer status;

  private String error;

  private String message;

  private String path;

  private String timestamp;

  /**
   * Default constructor
   * @deprecated Use {@link ErrorResponseServerModel#ErrorResponseServerModel(Integer, String, String, String, String)}
   */
  @Deprecated
  public ErrorResponseServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ErrorResponseServerModel(Integer status, String error, String message, String path, String timestamp) {
    this.status = status;
    this.error = error;
    this.message = message;
    this.path = path;
    this.timestamp = timestamp;
  }

  public ErrorResponseServerModel status(Integer status) {
    this.status = status;
    return this;
  }

  /**
   * HTTP status code
   * @return status
  */
  @NotNull 
  @Schema(name = "status", description = "HTTP status code", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("status")
  public Integer getStatus() {
    return status;
  }

  public void setStatus(Integer status) {
    this.status = status;
  }

  public ErrorResponseServerModel error(String error) {
    this.error = error;
    return this;
  }

  /**
   * error
   * @return error
  */
  @NotNull 
  @Schema(name = "error", description = "error", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("error")
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public ErrorResponseServerModel message(String message) {
    this.message = message;
    return this;
  }

  /**
   * error message
   * @return message
  */
  @NotNull 
  @Schema(name = "message", description = "error message", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ErrorResponseServerModel path(String path) {
    this.path = path;
    return this;
  }

  /**
   * requested record path
   * @return path
  */
  @NotNull 
  @Schema(name = "path", description = "requested record path", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("path")
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public ErrorResponseServerModel timestamp(String timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * time of error
   * @return timestamp
  */
  @NotNull 
  @Schema(name = "timestamp", description = "time of error", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("timestamp")
  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorResponseServerModel errorResponse = (ErrorResponseServerModel) o;
    return Objects.equals(this.status, errorResponse.status) &&
        Objects.equals(this.error, errorResponse.error) &&
        Objects.equals(this.message, errorResponse.message) &&
        Objects.equals(this.path, errorResponse.path) &&
        Objects.equals(this.timestamp, errorResponse.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, error, message, path, timestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ErrorResponseServerModel {\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    error: ").append(toIndentedString(error)).append("\n");
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
    sb.append("    path: ").append(toIndentedString(path)).append("\n");
    sb.append("    timestamp: ").append(toIndentedString(timestamp)).append("\n");
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

