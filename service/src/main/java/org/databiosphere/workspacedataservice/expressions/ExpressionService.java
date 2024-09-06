package org.databiosphere.workspacedataservice.expressions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ExpressionService {
  private final RecordDao recordDao;
  private final ObjectMapper objectMapper;
  private final AttributeLookupVisitor attributeLookupVisitor = new AttributeLookupVisitor();

  public ExpressionService(RecordDao recordDao, ObjectMapper objectMapper) {
    this.recordDao = recordDao;
    this.objectMapper = objectMapper;
  }

  public Map<String, JsonNode> evaluateExpressions(
      UUID collectionId,
      RecordType recordType,
      String recordId,
      Map<String, String> expressionsByName) {
    var attributeLookups =
        expressionsByName.values().stream()
            .map(this::extractRecordAttributeLookups)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    var expressionQueries =
        determineExpressionQueries(collectionId, recordType, attributeLookups, 0).toList();
    var resultsByQuery =
        executeExpressionQueries(collectionId, recordType, recordId, expressionQueries);
    return substituteResultsInExpressions(expressionsByName, resultsByQuery, recordType);
  }

  private Map<ExpressionQueryInfo, List<Record>> executeExpressionQueries(
      UUID collectionId,
      RecordType recordType,
      String recordId,
      List<ExpressionQueryInfo> expressionQueries) {
    return expressionQueries.stream()
        .map(
            expressionQueryInfo ->
                Map.entry(
                    expressionQueryInfo,
                    recordDao.queryRelatedRecords(
                        collectionId, recordType, recordId, expressionQueryInfo.relations())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  Map<String, JsonNode> substituteResultsInExpressions(
      Map<String, String> expressionsByName,
      Map<ExpressionQueryInfo, List<Record>> resultsByQuery,
      RecordType recordType) {
    var expressionValueSubstitutionVisitor =
        new ExpressionValueSubstitutionVisitor(
            getExpressionResultLookupMap(resultsByQuery, recordType), objectMapper);
    return expressionsByName.entrySet().stream()
        .map(
            expressionEntry -> {
              var parser = getParser(expressionEntry.getValue());
              var parsedTree = parser.root();
              return Map.entry(
                  expressionEntry.getKey(), expressionValueSubstitutionVisitor.visit(parsedTree));
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  Map<String, JsonNode> getExpressionResultLookupMap(
      Map<ExpressionQueryInfo, List<Record>> resultsByQuery, RecordType recordType) {
    return resultsByQuery.entrySet().stream()
        .flatMap(
            queryInfoAndResult ->
                queryInfoAndResult.getKey().attributeLookups().stream()
                    .map(
                        lookup -> {
                          var attributeValues =
                              queryInfoAndResult.getValue().stream()
                                  .map(
                                      record ->
                                          lookupAttributeValue(
                                              recordType, queryInfoAndResult, lookup, record))
                                  .filter(Objects::nonNull)
                                  .map(this::toJsonNode)
                                  .toList();
                          if (queryInfoAndResult.getKey().isArray()) {
                            return Map.entry(
                                lookup.lookupText(),
                                objectMapper.createArrayNode().addAll(attributeValues));
                          } else if (attributeValues.size() > 1) {
                            throw new ResponseStatusException(
                                HttpStatus.BAD_REQUEST,
                                "Expected a single value for attribute %s but got %s"
                                    .formatted(lookup.attribute(), attributeValues));
                          } else if (attributeValues.isEmpty()) {
                            return Map.entry(lookup.lookupText(), objectMapper.nullNode());
                          } else {
                            return Map.entry(lookup.lookupText(), attributeValues.get(0));
                          }
                        }))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Object lookupAttributeValue(
      RecordType recordType,
      Map.Entry<ExpressionQueryInfo, List<Record>> queryInfoAndResult,
      AttributeLookup lookup,
      Record record) {
    return lookup.attribute().equalsIgnoreCase(getIdName(recordType, queryInfoAndResult))
        ? record.getId()
        : record.getAttributeValue(lookup.attribute());
  }

  private String getIdName(
      RecordType recordType, Map.Entry<ExpressionQueryInfo, List<Record>> queryInfoAndResult) {
    var relations = queryInfoAndResult.getKey().relations();
    return (relations.isEmpty()
            ? recordType.getName()
            : relations.get(relations.size() - 1).relationRecordType().getName())
        + "_id";
  }

  private JsonNode toJsonNode(Object attributeValue) {
    if (attributeValue == null) {
      return objectMapper.nullNode();
    } else if (attributeValue instanceof JsonNode) {
      return (JsonNode) attributeValue;
    } else {
      return objectMapper.valueToTree(attributeValue);
    }
  }

  /**
   * Extracts all attribute lookups from an expression. An expressions may have multiple lookups.
   *
   * @param expression
   * @return
   */
  Set<AttributeLookup> extractRecordAttributeLookups(String expression) {
    var parser = getParser(expression);
    var parsedTree = parser.root();
    return attributeLookupVisitor.visit(parsedTree);
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
    var arrayRelationColsMap =
        recordDao.getRelationArrayCols(collectionId, recordType).stream()
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
    missingRelations.removeAll(arrayRelationColsMap.keySet());
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
                relationAndLookup -> {
                  boolean isArray = arrayRelationColsMap.containsKey(relationAndLookup.getKey());
                  Relation relation =
                      isArray
                          ? arrayRelationColsMap.get(relationAndLookup.getKey())
                          : relationColsMap.get(relationAndLookup.getKey());
                  return determineExpressionQueries(
                          collectionId,
                          relation.relationRecordType(),
                          relationAndLookup.getValue(),
                          relationLevel + 1)
                      .map(queryInfo -> queryInfo.prependRelation(relation, isArray));
                });

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
            Stream.of(new ExpressionQueryInfo(List.of(), currentLookups, false)),
            nextExpressionQueries);
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
