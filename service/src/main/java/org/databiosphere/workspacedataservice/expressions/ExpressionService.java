package org.databiosphere.workspacedataservice.expressions;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.expressions.parser.antlr.TerraExpressionLexer;
import org.databiosphere.workspacedataservice.expressions.parser.antlr.TerraExpressionParser;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ExpressionService {
  private final RecordDao recordDao;

  public ExpressionService(RecordDao recordDao) {
    this.recordDao = recordDao;
  }

  Set<AttributeLookup> extractRecordAttributeLookups(String expression) {
    var parser = getParser(expression);
    var parsedTree = parser.root();
    var visitor = new AttributeLookupVisitor();
    return visitor.visit(parsedTree);
  }

  /**
   * This is a recursive function that walks down the relations lists of each attributeLookups. Each
   * relation should correspond to a relation column between record types. We start with
   * relationLevel 0. All attribute lookups that have the same number of relations as the relation
   * level (none to start) are queries on the given record type. Collect those attributes and put
   * them in an ExpressionQueryInfo with no relations. For all lookups that have more relations than
   * relation level, group them by the relation at that level (the first relation to start) and call
   * this method recursively for each grouping, prepending the relation to the resulting
   * ExpressionQueryInfo to build the relation chain.
   *
   * @param collectionId
   * @param recordType
   * @param attributeLookups
   * @param relationLevel
   * @return
   */
  Stream<ExpressionQueryInfo> determineExpressionQueries(
      UUID collectionId,
      RecordType recordType,
      Collection<AttributeLookup> attributeLookups,
      int relationLevel) {
    var relationColsMap =
        recordDao.getRelationCols(collectionId, recordType).stream()
            .collect(Collectors.toMap(Relation::relationColName, Function.identity()));

    // group all lookups that have the same relation at relationLevel together
    // this grouping makes sure that subsequent recursive calls to this function have the
    // same relations up to this point
    var nextLookupByRelation =
        attributeLookups.stream()
            .filter(l -> l.relations().size() > relationLevel)
            .collect(Collectors.groupingBy(l -> l.relations().get(relationLevel)));

    var missingRelations = new HashSet<>(nextLookupByRelation.keySet());
    missingRelations.removeAll(relationColsMap.keySet());
    if (!missingRelations.isEmpty()) {
      var badExpressions =
          attributeLookups.stream()
              .filter(l -> missingRelations.contains(l.relations().get(relationLevel)))
              .map(AttributeLookup::lookupText)
              .toList();
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "The relations %s in expressions %s do not exist"
              .formatted(missingRelations, badExpressions));
    }

    // for each group call this function recursively incrementing the relationLevel and prepend the
    // relation for the group to the result
    var nextExpressionQueries =
        nextLookupByRelation.entrySet().stream()
            .flatMap(
                relationAndLookup ->
                    determineExpressionQueries(
                            collectionId,
                            relationColsMap.get(relationAndLookup.getKey()).relationRecordType(),
                            relationAndLookup.getValue(),
                            relationLevel + 1)
                        .map(
                            queryInfo ->
                                queryInfo.prependRelation(
                                    relationColsMap.get(relationAndLookup.getKey()))));

    var currentLookups =
        attributeLookups.stream()
            .filter(l -> l.relations().size() == relationLevel)
            .collect(Collectors.toSet());

    // if there are no more lookups at this level, return the next level only
    // no lookups is effectively an empty select clause and happens when a record is use only for
    // traversal
    return currentLookups.isEmpty()
        ? nextExpressionQueries
        : Stream.concat(
            Stream.of(new ExpressionQueryInfo(List.of(), currentLookups)), nextExpressionQueries);
  }

  private TerraExpressionParser getParser(String expression) {
    var errorThrowingListener = new ErrorThrowingListener();
    CodePointCharStream inputStream = CharStreams.fromString(expression);

    TerraExpressionLexer lexer = new TerraExpressionLexer(inputStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorThrowingListener);

    var tokenStream = new CommonTokenStream(lexer);
    TerraExpressionParser parser = new TerraExpressionParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(errorThrowingListener);

    return parser;
  }
}
