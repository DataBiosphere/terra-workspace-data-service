package org.databiosphere.workspacedataservice.generated;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.databiosphere.workspacedataservice.generated.NamedExpressionServerModel;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import jakarta.annotation.Generated;

/**
 * EvaluateExpressionsRequestServerModel
 */

@JsonTypeName("EvaluateExpressionsRequest")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
public class EvaluateExpressionsRequestServerModel {

  @Valid
  private List<@Valid NamedExpressionServerModel> expressions = new ArrayList<>();

  public EvaluateExpressionsRequestServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public EvaluateExpressionsRequestServerModel(List<@Valid NamedExpressionServerModel> expressions) {
    this.expressions = expressions;
  }

  public EvaluateExpressionsRequestServerModel expressions(List<@Valid NamedExpressionServerModel> expressions) {
    this.expressions = expressions;
    return this;
  }

  public EvaluateExpressionsRequestServerModel addExpressionsItem(NamedExpressionServerModel expressionsItem) {
    if (this.expressions == null) {
      this.expressions = new ArrayList<>();
    }
    this.expressions.add(expressionsItem);
    return this;
  }

  /**
   * List of expressions to evaluate. The expression \"this.foo\" will get the value of the attribute \"foo\" in the record. The expression \"this.relation.foo\" will get the value of the attribute \"foo\" from the related record specified by attribute \"relation\" in the record. The expression \"{'name': this.foo, 'num': this.bar }\" will create a JSON object with key \"name\" and value of the attribute \"foo\" and the key \"num\" and value of the attribute \"bar. 
   * @return expressions
   */
  @NotNull @Valid 
  @Schema(name = "expressions", description = "List of expressions to evaluate. The expression \"this.foo\" will get the value of the attribute \"foo\" in the record. The expression \"this.relation.foo\" will get the value of the attribute \"foo\" from the related record specified by attribute \"relation\" in the record. The expression \"{'name': this.foo, 'num': this.bar }\" will create a JSON object with key \"name\" and value of the attribute \"foo\" and the key \"num\" and value of the attribute \"bar. ", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("expressions")
  public List<@Valid NamedExpressionServerModel> getExpressions() {
    return expressions;
  }

  public void setExpressions(List<@Valid NamedExpressionServerModel> expressions) {
    this.expressions = expressions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EvaluateExpressionsRequestServerModel evaluateExpressionsRequest = (EvaluateExpressionsRequestServerModel) o;
    return Objects.equals(this.expressions, evaluateExpressionsRequest.expressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressions);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EvaluateExpressionsRequestServerModel {\n");
    sb.append("    expressions: ").append(toIndentedString(expressions)).append("\n");
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

