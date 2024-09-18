package org.databiosphere.workspacedataservice.expressions;

import bio.terra.common.db.ReadTransaction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayResponseServerModel;
import org.databiosphere.workspacedataservice.generated.ExpressionEvaluationsForRecordServerModel;
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

  /**
   * Evaluates a set of expressions on a record. The expressions are expected parsed by the
   * ANTLR-generated parser and visitor classes. Examples of expressions include:
   *
   * <ul>
   *   <li>this.attribute
   *   <li>this.relation.attribute
   *   <li>{"key": this.attribute, "key2": this.relation.attribute}
   * </ul>
   *
   * @param collectionId The collection id
   * @param recordType The record type
   * @param recordId The record id to evaluate the expressions on
   * @param expressionsByName A map of expression names to expressions
   * @return A map of expression names to the evaluated expressions
   */
  @ReadTransaction
  public EvaluateExpressionsResponseServerModel evaluateExpressions(
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
    return new EvaluateExpressionsResponseServerModel()
        .evaluations(substituteResultsInExpressions(expressionsByName, resultsByQuery, recordType));
  }

  /**
   * Like evaluateExpressions but for an array of records instead of a single record. The array is
   * specified by the arrayRecordType, arrayRecordId, and arrayRelationExpression. The
   * arrayRelationExpression is a string that specifies the relations to follow to get to the array
   * of records. An example of arrayRelationExpression is "this.relation1.relation2". At least one
   * of the relations in the expression should be an array.
   *
   * @param collectionId The collection id
   * @param arrayRecordType The record type of the array of records
   * @param arrayRecordId The record id of the array of records
   * @param arrayRelationExpression The expression to get to the array of records
   * @param expressionsByName A map of expression names to expressions
   * @param pageSize The number of array records to evaluate expressions against
   * @param offset The offset into the array of records
   * @return A map of maps: array record id -> expression name -> evaluated expression
   */
  @ReadTransaction
  public EvaluateExpressionsWithArrayResponseServerModel evaluateExpressionsWithRelationArray(
      UUID collectionId,
      RecordType arrayRecordType,
      String arrayRecordId,
      String arrayRelationExpression,
      Map<String, String> expressionsByName,
      int pageSize,
      int offset) {
    var arrayRelations = getArrayRelations(collectionId, arrayRecordType, arrayRelationExpression);
    var queryRecordType = arrayRelations.get(arrayRelations.size() - 1).relationRecordType();

    var attributeLookups =
        expressionsByName.values().stream()
            .map(this::extractRecordAttributeLookups)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    var expressionQueries =
        determineExpressionQueries(collectionId, queryRecordType, attributeLookups, 0).toList();
    var resultsByQuery =
        executeExpressionQueriesWithRelationArray(
            collectionId,
            arrayRecordType,
            arrayRecordId,
            arrayRelations,
            expressionQueries,
            // plus one to see if there are more records
            pageSize + 1,
            offset);
    // the query results may have an extra record to see if there are more records
    // if there are more records, remove the extra record
    var hasNext =
        resultsByQuery.values().stream().map(LinkedHashMap::size).anyMatch(s -> s > pageSize);
    if (hasNext) {
      removeExtraRecords(pageSize, resultsByQuery);
    }

    // resultsByQuery is a map of maps of expression query info -> record id -> query result,
    // but we need to flip the keys to get the results in the shape that we want
    return new EvaluateExpressionsWithArrayResponseServerModel()
        .hasNext(hasNext)
        .results(
            transposeMapKeys(resultsByQuery).entrySet().stream()
                .map(
                    entry ->
                        new ExpressionEvaluationsForRecordServerModel()
                            .recordId(entry.getKey())
                            .evaluations(
                                substituteResultsInExpressions(
                                    expressionsByName, entry.getValue(), queryRecordType)))
                .toList());
  }

  /**
   * For each result, remove the extra record if there are more records than the page size. This
   * works because the LinkedHashMap maintains insertion order.
   */
  private static void removeExtraRecords(
      int pageSize, Map<ExpressionQueryInfo, LinkedHashMap<String, List<Record>>> resultsByQuery) {
    resultsByQuery
        .values()
        .forEach(
            m -> {
              var keyArray = m.keySet().toArray(new String[0]);
              if (keyArray.length > pageSize) {
                m.remove(keyArray[pageSize]);
              }
            });
  }

  /**
   * Parse the arrayRelationExpression to get the relations to follow to get to the array of
   * records.
   */
  @VisibleForTesting
  List<Relation> getArrayRelations(
      UUID collectionId, RecordType arrayRecordType, String arrayRelationExpression) {
    var arrayRelationAttributeLookups = extractRecordAttributeLookups(arrayRelationExpression);
    if (arrayRelationAttributeLookups.size() != 1) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "Expected a single expression in %s".formatted(arrayRelationExpression));
    }
    var arrayRelationLookup = arrayRelationAttributeLookups.iterator().next();
    List<Relation> arrayRelations = new ArrayList<>();
    // start with the record type of the array of records
    var priorRecordType = arrayRecordType;
    for (var relationName : arrayRelationLookup.relations()) {
      var relation =
          getRelation(collectionId, arrayRelationExpression, relationName, priorRecordType);

      arrayRelations.add(relation);
      // update the prior record type for the next relation
      priorRecordType = relation.relationRecordType();
    }
    // for the array relation lookup, the attribute must be a relation too
    arrayRelations.add(
        getRelation(
            collectionId,
            arrayRelationExpression,
            arrayRelationLookup.attribute(),
            priorRecordType));
    return arrayRelations;
  }

  /**
   * Get the relation by name. If the relation is not found, throw an exception. Relation could be a
   * scalar or array relation.
   */
  private Relation getRelation(
      UUID collectionId,
      String arrayRelationExpression,
      String relationName,
      RecordType priorRecordType) {
    return Stream.concat(
            recordDao.getRelationCols(collectionId, priorRecordType).stream(),
            recordDao.getRelationArrayCols(collectionId, priorRecordType).stream())
        .filter(r -> r.relationColName().equals(relationName))
        .findFirst()
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "The relation %s in expression %s does not exist"
                        .formatted(relationName, arrayRelationExpression)));
  }

  /**
   * Convert the resultsByQuery containing expression query info -> record id -> query result to
   * results record id -> expression name -> query result. Thanks CoPilot.
   */
  private Map<String, Map<ExpressionQueryInfo, List<Record>>> transposeMapKeys(
      Map<ExpressionQueryInfo, LinkedHashMap<String, List<Record>>> resultsByQuery) {
    Map<String, Map<ExpressionQueryInfo, List<Record>>> resultsByArrayRecordId = new HashMap<>();

    for (Map.Entry<ExpressionQueryInfo, LinkedHashMap<String, List<Record>>> outerEntry :
        resultsByQuery.entrySet()) {
      ExpressionQueryInfo outerKey = outerEntry.getKey();
      Map<String, List<Record>> innerMap = outerEntry.getValue();

      for (Map.Entry<String, List<Record>> innerEntry : innerMap.entrySet()) {
        String innerKey = innerEntry.getKey();
        List<Record> records = innerEntry.getValue();

        resultsByArrayRecordId
            .computeIfAbsent(innerKey, k -> new HashMap<>())
            .put(outerKey, records);
      }
    }
    return resultsByArrayRecordId;
  }

  /**
   * Execute the expression queries on a single record.
   *
   * @return A map of expression query info -> query result
   */
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

  /**
   * Execute the expression queries on an array of records.
   *
   * @return A map of expression query info -> record id -> query result
   */
  private Map<ExpressionQueryInfo, LinkedHashMap<String, List<Record>>>
      executeExpressionQueriesWithRelationArray(
          UUID collectionId,
          RecordType arrayRecordType,
          String arrayRecordId,
          List<Relation> arrayRelations,
          List<ExpressionQueryInfo> expressionQueries,
          int pageSize,
          int offset) {
    return expressionQueries.stream()
        .map(
            expressionQueryInfo ->
                Map.entry(
                    expressionQueryInfo,
                    recordDao.queryRelatedRecordsWithArray(
                        collectionId,
                        arrayRecordType,
                        arrayRecordId,
                        arrayRelations,
                        expressionQueryInfo.relations(),
                        pageSize,
                        offset)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Substitute the results of the expression queries into the expressions.
   *
   * @return A map of expression name -> expression with lookups replaced by values
   */
  @VisibleForTesting
  Map<String, Object> substituteResultsInExpressions(
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

  /**
   * Generate a map of lookup text -> expression result for each expression. The expression result
   * is a JsonNode which can be a null node, a single value, or an array of values.
   *
   * <ul>
   *   <li>null node when query info indicates that the results should not be an array AND
   *       <ul>
   *         <li>the results contain an empty list of records OR
   *         <li>the attribute does not exist on any record
   *       </ul>
   *   <li>single value when query info indicates that results should not be an array AND the
   *       results contain a single record with the attribute
   *   <li>array of 0 or more elements when query info indicates that the attribute is an array
   * </ul>
   *
   * @param resultsByQuery A map of expression query info -> query result. Each query info may
   *     contain multiple attribute lookups and each lookup has its own text value which will be a
   *     key in the result.
   * @param recordType The record type the expressions were evaluated against
   */
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
                                              recordType,
                                              lookup,
                                              record,
                                              queryInfoAndResult.getKey().relations()))
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

  /**
   * If the attribute is of the form [recordType]_id, return the record id. Otherwise, return the
   * attribute value.
   */
  private Object lookupAttributeValue(
      RecordType recordType, AttributeLookup lookup, Record record, List<Relation> relations) {
    return lookup.attribute().equalsIgnoreCase(getIdName(recordType, relations))
        ? record.getId()
        : record.getAttributeValue(lookup.attribute());
  }

  /** Get the name of the id attribute for the record type in the form [recordType]_id. */
  private String getIdName(RecordType recordType, List<Relation> relations) {
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
   * The expression this.relation.attribute has only one lookup, itself. The expression {"key":
   * this.attribute, "key2": this.relation.attribute} has two lookups: this.attribute and
   * this.relation.attribute.
   */
  @VisibleForTesting
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
   * @param collectionId The collection id
   * @param recordType The record type of the current relationLevel
   * @param attributeLookups The attribute lookups to process, all of which must have the same value
   *     up to relationLevel
   * @param relationLevel The current relation level starting with 0 and incrementing with each
   *     recursive call
   * @return A stream of ExpressionQueryInfo objects that represent the queries to execute
   */
  @VisibleForTesting
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
