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
 * GenericJobAllOfServerModel
 */

@JsonTypeName("GenericJob_allOf")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen")
public class GenericJobAllOfServerModel {

  private Object input;

  private Object result;

  public GenericJobAllOfServerModel input(Object input) {
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

  public GenericJobAllOfServerModel result(Object result) {
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
    GenericJobAllOfServerModel genericJobAllOf = (GenericJobAllOfServerModel) o;
    return Objects.equals(this.input, genericJobAllOf.input) &&
        Objects.equals(this.result, genericJobAllOf.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(input, result);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GenericJobAllOfServerModel {\n");
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

