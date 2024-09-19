package org.databiosphere.workspacedataservice.expressions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.databiosphere.workspacedataservice.expressions.parser.antlr.TerraExpressionBaseVisitor;
import org.databiosphere.workspacedataservice.expressions.parser.antlr.TerraExpressionParser;

public class ExpressionValueSubstitutionVisitor extends TerraExpressionBaseVisitor<JsonNode> {
  private final Map<String, JsonNode> lookupMap;
  private final ObjectMapper objectMapper;

  public ExpressionValueSubstitutionVisitor(
      Map<String, JsonNode> lookupMap, ObjectMapper objectMapper) {
    this.lookupMap = lookupMap;
    this.objectMapper = objectMapper;
  }

  @Override
  public JsonNode visitRoot(TerraExpressionParser.RootContext ctx) {
    return visit(ctx.getChild(0));
  }

  @Override
  public JsonNode visitObj(TerraExpressionParser.ObjContext ctx) {
    ObjectNode objNode = objectMapper.createObjectNode();
    ctx.pair()
        .forEach(
            pairContext -> {
              JsonNode pairNode = visit(pairContext);
              pairNode
                  .fields()
                  .forEachRemaining(entry -> objNode.set(entry.getKey(), entry.getValue()));
            });
    return objNode;
  }

  @Override
  public JsonNode visitPair(TerraExpressionParser.PairContext ctx) {
    String quotedKeyString = ctx.STRING().getText();
    String unquotedKeyString = quotedKeyString.substring(1, quotedKeyString.length() - 1);
    JsonNode childValue = visit(ctx.value());
    ObjectNode pairNode = objectMapper.createObjectNode();
    pairNode.set(unquotedKeyString, childValue);
    return pairNode;
  }

  @Override
  public JsonNode visitArr(TerraExpressionParser.ArrContext ctx) {
    ArrayNode arrayNode = objectMapper.createArrayNode();
    ctx.value().forEach(valueContext -> arrayNode.add(visit(valueContext)));
    return arrayNode;
  }

  @Override
  public JsonNode visitLookup(TerraExpressionParser.LookupContext ctx) {
    return lookupMap.getOrDefault(ctx.getText(), objectMapper.nullNode());
  }

  @Override
  public JsonNode visitValue(TerraExpressionParser.ValueContext ctx) {
    return visit(ctx.getChild(0));
  }

  @Override
  public JsonNode visitLiteral(TerraExpressionParser.LiteralContext ctx) {
    try {
      return objectMapper.readTree(ctx.getText());
    } catch (Exception e) {
      throw new RuntimeException("Error parsing literal: " + ctx.getText(), e);
    }
  }
}
