package org.databiosphere.workspacedataservice.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.databiosphere.workspacedataservice.service.model.Relation;

public record ExpressionQueryInfo(
    List<Relation> relations, Set<AttributeLookup> attributeLookups, boolean isArray) {

  public ExpressionQueryInfo prependRelation(Relation relation, boolean isArray) {
    var updatedRelations = new ArrayList<>(relations);
    updatedRelations.add(0, relation);
    return new ExpressionQueryInfo(updatedRelations, attributeLookups, this.isArray || isArray);
  }
}
