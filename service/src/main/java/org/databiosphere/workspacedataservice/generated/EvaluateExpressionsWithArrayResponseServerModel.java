package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.databiosphere.workspacedataservice.generated.ExpressionEvaluationsForRecordServerModel;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * EvaluateExpressionsWithArrayResponseServerModel
 */

@JsonTypeName("EvaluateExpressionsWithArrayResponse")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.10.0")
public class EvaluateExpressionsWithArrayResponseServerModel {

  @Valid
  private List<@Valid ExpressionEvaluationsForRecordServerModel> results = new ArrayList<>();

  private Boolean hasNext;

  public EvaluateExpressionsWithArrayResponseServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public EvaluateExpressionsWithArrayResponseServerModel(List<@Valid ExpressionEvaluationsForRecordServerModel> results, Boolean hasNext) {
    this.results = results;
    this.hasNext = hasNext;
  }

  public EvaluateExpressionsWithArrayResponseServerModel results(List<@Valid ExpressionEvaluationsForRecordServerModel> results) {
    this.results = results;
    return this;
  }

  public EvaluateExpressionsWithArrayResponseServerModel addResultsItem(ExpressionEvaluationsForRecordServerModel resultsItem) {
    if (this.results == null) {
      this.results = new ArrayList<>();
    }
    this.results.add(resultsItem);
    return this;
  }

  /**
   * Get results
   * @return results
   */
  @NotNull @Valid 
  @Schema(name = "results", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("results")
  public List<@Valid ExpressionEvaluationsForRecordServerModel> getResults() {
    return results;
  }

  public void setResults(List<@Valid ExpressionEvaluationsForRecordServerModel> results) {
    this.results = results;
  }

  public EvaluateExpressionsWithArrayResponseServerModel hasNext(Boolean hasNext) {
    this.hasNext = hasNext;
    return this;
  }

  /**
   * Indicates if there are more records to evaluate expressions on. 
   * @return hasNext
   */
  @NotNull 
  @Schema(name = "hasNext", description = "Indicates if there are more records to evaluate expressions on. ", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("hasNext")
  public Boolean getHasNext() {
    return hasNext;
  }

  public void setHasNext(Boolean hasNext) {
    this.hasNext = hasNext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EvaluateExpressionsWithArrayResponseServerModel evaluateExpressionsWithArrayResponse = (EvaluateExpressionsWithArrayResponseServerModel) o;
    return Objects.equals(this.results, evaluateExpressionsWithArrayResponse.results) &&
        Objects.equals(this.hasNext, evaluateExpressionsWithArrayResponse.hasNext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(results, hasNext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EvaluateExpressionsWithArrayResponseServerModel {\n");
    sb.append("    results: ").append(toIndentedString(results)).append("\n");
    sb.append("    hasNext: ").append(toIndentedString(hasNext)).append("\n");
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

