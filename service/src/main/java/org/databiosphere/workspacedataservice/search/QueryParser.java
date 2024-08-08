package org.databiosphere.workspacedataservice.search;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class QueryParser {

  public static final String DEFAULT_ALL_COLUMNS_NAME = "sys_all_columns";

  private final Map<String, DataTypeMapping> schema;

  public QueryParser(Map<String, DataTypeMapping> schema) {
    this.schema = schema;
  }

  public WhereClausePart parse(String query) {
    // query should not be blank by the time we get here, but if it is, short-circuit and return
    // an empty clause.
    if (StringUtils.isBlank(query)) {
      return new WhereClausePart(List.of(), Map.of());
    }
    // create a Lucene parser
    StandardSyntaxParser standardSyntaxParser = new StandardSyntaxParser();

    // attempt to parse
    QueryNode parsed;
    try {
      parsed = standardSyntaxParser.parse(query, DEFAULT_ALL_COLUMNS_NAME);
    } catch (QueryNodeParseException queryNodeParseException) {
      throw new InvalidQueryException();
    }

    // even if the query parsed correctly via the Lucene library, ensure the query does not use any
    // syntax that WDS doesn't support. For now, we only support a single FieldQueryNode.
    if (parsed instanceof FieldQueryNode fieldQueryNode) {
      String column = fieldQueryNode.getFieldAsString();
      String value = fieldQueryNode.getTextAsString();

      validateColumnName(column);

      // determine the datatype of the column on which we are filtering.
      var datatype = schema.get(column);

      // init our return values. In the future we want to support filtering on multiple columns,
      // so we need to support a list of clause fragments.
      List<String> clauses = new ArrayList<>();
      Map<String, Object> values = new HashMap<>();

      // bind parameter names have syntax limitations, so we use artificial ones below
      var paramName = "filterquery0";

      // based on the datatype of the column, build relevant SQL
      switch (datatype) {
        case STRING:
          // LOWER("mycolumn") = 'mysearchterm'
          clauses.add("LOWER(" + quote(column) + ") = :" + paramName);
          values.put(paramName, value.toLowerCase());
          break;
        case ARRAY_OF_STRING:
          // 'mysearchterm' ILIKE ANY("mycolumn")
          clauses.add(":" + paramName + " ILIKE ANY(" + quote(column) + ")");
          values.put(paramName, value.toLowerCase());
          break;
        case NUMBER:
          // "mycolumn" = 42
          clauses.add(quote(column) + " = :" + paramName);
          values.put(paramName, parseNumericValue(value));
          break;
        case ARRAY_OF_NUMBER:
          // 42 = ANY("mycolumn")
          clauses.add(":" + paramName + " = ANY(" + quote(column) + ")");
          values.put(paramName, parseNumericValue(value));
          break;
        default:
          throw new InvalidQueryException("Column specified in query must be a string type");
      }

      return new WhereClausePart(clauses, values);
    } else {
      throw new InvalidQueryException();
    }
  }

  private Double parseNumericValue(String value) {
    BigDecimal parsedNumber;
    try {
      parsedNumber = new BigDecimal(value);
    } catch (NumberFormatException nfe) {
      throw new InvalidQueryException("Query value for numeric column must be a number");
    }
    return parsedNumber.doubleValue();
  }

  private void validateColumnName(String columnName) {
    // The Lucene query parser requires a default column name to parse a query. If the end user
    // has not specified a column, the query parser will use the default column name. In our case,
    // if we see the default column name we consider it an error - as of this writing, we require
    // the end user to specify a column name.
    if (DEFAULT_ALL_COLUMNS_NAME.equals(columnName)) {
      throw new InvalidQueryException("Query must specify a column name");
    }
    // does this column exist in the record type?
    if (!schema.containsKey(columnName)) {
      throw new InvalidQueryException(
          "Column specified in query does not exist in this record type");
    }
  }
}
