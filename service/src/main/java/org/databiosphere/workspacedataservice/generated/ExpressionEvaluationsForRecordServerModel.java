package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.HashMap;
import java.util.Map;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * ExpressionEvaluationsForRecordServerModel
 */

@JsonTypeName("ExpressionEvaluationsForRecord")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class ExpressionEvaluationsForRecordServerModel {

  private String recordId;

  @Valid
  private Map<String, Object> evaluations = new HashMap<>();

  public ExpressionEvaluationsForRecordServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ExpressionEvaluationsForRecordServerModel(String recordId, Map<String, Object> evaluations) {
    this.recordId = recordId;
    this.evaluations = evaluations;
  }

  public ExpressionEvaluationsForRecordServerModel recordId(String recordId) {
    this.recordId = recordId;
    return this;
  }

  /**
   * Record id
   * @return recordId
   */
  @NotNull 
  @Schema(name = "recordId", description = "Record id", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("recordId")
  public String getRecordId() {
    return recordId;
  }

  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  public ExpressionEvaluationsForRecordServerModel evaluations(Map<String, Object> evaluations) {
    this.evaluations = evaluations;
    return this;
  }

  public ExpressionEvaluationsForRecordServerModel putEvaluationsItem(String key, Object evaluationsItem) {
    if (this.evaluations == null) {
      this.evaluations = new HashMap<>();
    }
    this.evaluations.put(key, evaluationsItem);
    return this;
  }

  /**
   * The key is the expression name and the value is the result of the expression. 
   * @return evaluations
   */
  @NotNull 
  @Schema(name = "evaluations", description = "The key is the expression name and the value is the result of the expression. ", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("evaluations")
  public Map<String, Object> getEvaluations() {
    return evaluations;
  }

  public void setEvaluations(Map<String, Object> evaluations) {
    this.evaluations = evaluations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionEvaluationsForRecordServerModel expressionEvaluationsForRecord = (ExpressionEvaluationsForRecordServerModel) o;
    return Objects.equals(this.recordId, expressionEvaluationsForRecord.recordId) &&
        Objects.equals(this.evaluations, expressionEvaluationsForRecord.evaluations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordId, evaluations);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ExpressionEvaluationsForRecordServerModel {\n");
    sb.append("    recordId: ").append(toIndentedString(recordId)).append("\n");
    sb.append("    evaluations: ").append(toIndentedString(evaluations)).append("\n");
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

