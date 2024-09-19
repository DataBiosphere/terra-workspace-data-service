package org.databiosphere.workspacedataservice.expressions;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.expressions.parser.antlr.TerraExpressionBaseVisitor;
import org.databiosphere.workspacedataservice.expressions.parser.antlr.TerraExpressionParser;

public class AttributeLookupVisitor extends TerraExpressionBaseVisitor<Set<AttributeLookup>> {
  @Override
  protected Set<AttributeLookup> aggregateResult(
      Set<AttributeLookup> aggregate, Set<AttributeLookup> nextResult) {
    return Stream.of(aggregate, nextResult).flatMap(Set::stream).collect(Collectors.toSet());
  }

  @Override
  protected Set<AttributeLookup> defaultResult() {
    return Set.of();
  }

  @Override
  public Set<AttributeLookup> visitEntityLookup(TerraExpressionParser.EntityLookupContext ctx) {
    // note that this ignores namespaces in attribute names which WDS does not support
    return Set.of(
        new AttributeLookup(
            ctx.relation().stream()
                .map(r -> r.attributeName().name().getText())
                .collect(Collectors.toList()),
            ctx.attributeName().name().getText(),
            ctx.getText()));
  }
}
