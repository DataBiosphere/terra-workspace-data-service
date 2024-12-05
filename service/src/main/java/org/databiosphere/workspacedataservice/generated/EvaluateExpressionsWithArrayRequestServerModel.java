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
 * EvaluateExpressionsWithArrayRequestServerModel
 */

@JsonTypeName("EvaluateExpressionsWithArrayRequest")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.9.0")
public class EvaluateExpressionsWithArrayRequestServerModel {

  @Valid
  private List<@Valid NamedExpressionServerModel> expressions = new ArrayList<>();

  private String arrayExpression;

  private Integer pageSize;

  private Integer offset;

  public EvaluateExpressionsWithArrayRequestServerModel() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public EvaluateExpressionsWithArrayRequestServerModel(List<@Valid NamedExpressionServerModel> expressions, String arrayExpression, Integer pageSize, Integer offset) {
    this.expressions = expressions;
    this.arrayExpression = arrayExpression;
    this.pageSize = pageSize;
    this.offset = offset;
  }

  public EvaluateExpressionsWithArrayRequestServerModel expressions(List<@Valid NamedExpressionServerModel> expressions) {
    this.expressions = expressions;
    return this;
  }

  public EvaluateExpressionsWithArrayRequestServerModel addExpressionsItem(NamedExpressionServerModel expressionsItem) {
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

  public EvaluateExpressionsWithArrayRequestServerModel arrayExpression(String arrayExpression) {
    this.arrayExpression = arrayExpression;
    return this;
  }

  /**
   * Expression to evaluate that returns an array of record ids. The expression \"this.relation\" will get the value of the attribute \"relation\" in the record. The expression \"this.relation.foo\" will get the value of the attribute \"foo\" from the related record specified by attribute \"relation\" in the record. 
   * @return arrayExpression
   */
  @NotNull 
  @Schema(name = "arrayExpression", description = "Expression to evaluate that returns an array of record ids. The expression \"this.relation\" will get the value of the attribute \"relation\" in the record. The expression \"this.relation.foo\" will get the value of the attribute \"foo\" from the related record specified by attribute \"relation\" in the record. ", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("arrayExpression")
  public String getArrayExpression() {
    return arrayExpression;
  }

  public void setArrayExpression(String arrayExpression) {
    this.arrayExpression = arrayExpression;
  }

  public EvaluateExpressionsWithArrayRequestServerModel pageSize(Integer pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  /**
   * Number of records within the array to evaluate the expressions on. 
   * @return pageSize
   */
  @NotNull 
  @Schema(name = "pageSize", description = "Number of records within the array to evaluate the expressions on. ", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("pageSize")
  public Integer getPageSize() {
    return pageSize;
  }

  public void setPageSize(Integer pageSize) {
    this.pageSize = pageSize;
  }

  public EvaluateExpressionsWithArrayRequestServerModel offset(Integer offset) {
    this.offset = offset;
    return this;
  }

  /**
   * Number of records within the array to skip before returning the next page. 
   * @return offset
   */
  @NotNull 
  @Schema(name = "offset", description = "Number of records within the array to skip before returning the next page. ", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("offset")
  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EvaluateExpressionsWithArrayRequestServerModel evaluateExpressionsWithArrayRequest = (EvaluateExpressionsWithArrayRequestServerModel) o;
    return Objects.equals(this.expressions, evaluateExpressionsWithArrayRequest.expressions) &&
        Objects.equals(this.arrayExpression, evaluateExpressionsWithArrayRequest.arrayExpression) &&
        Objects.equals(this.pageSize, evaluateExpressionsWithArrayRequest.pageSize) &&
        Objects.equals(this.offset, evaluateExpressionsWithArrayRequest.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressions, arrayExpression, pageSize, offset);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EvaluateExpressionsWithArrayRequestServerModel {\n");
    sb.append("    expressions: ").append(toIndentedString(expressions)).append("\n");
    sb.append("    arrayExpression: ").append(toIndentedString(arrayExpression)).append("\n");
    sb.append("    pageSize: ").append(toIndentedString(pageSize)).append("\n");
    sb.append("    offset: ").append(toIndentedString(offset)).append("\n");
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

