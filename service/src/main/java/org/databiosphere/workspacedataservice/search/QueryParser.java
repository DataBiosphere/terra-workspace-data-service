package org.databiosphere.workspacedataservice.search;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
        case STRING, FILE, RELATION -> {
          // LOWER("mycolumn") = 'mysearchterm'
          clauses.add("LOWER(" + quote(column) + ") = :" + paramName);
          values.put(paramName, value.toLowerCase());
        }
        case ARRAY_OF_STRING, ARRAY_OF_FILE -> {
          // 'mysearchterm' ILIKE ANY("mycolumn")
          clauses.add(":" + paramName + " ILIKE ANY(" + quote(column) + ")");
          values.put(paramName, value.toLowerCase());
        }
        case NUMBER -> {
          // "mycolumn" = 42
          clauses.add(quote(column) + " = :" + paramName);
          values.put(paramName, parseNumericValue(value));
        }
        case ARRAY_OF_NUMBER -> {
          // 42 = ANY("mycolumn")
          clauses.add(":" + paramName + " = ANY(" + quote(column) + ")");
          values.put(paramName, parseNumericValue(value));
        }
        case BOOLEAN -> {
          // "mycolumn" = false
          clauses.add(quote(column) + " = :" + paramName);
          values.put(paramName, strictParseBoolean(value));
        }
        case ARRAY_OF_BOOLEAN -> {
          // false = ANY("mycolumn")
          clauses.add(":" + paramName + " = ANY(" + quote(column) + ")");
          values.put(paramName, strictParseBoolean(value));
        }
        case DATE -> {
          // "mycolumn" = '1981-02-12'
          clauses.add(quote(column) + " = :" + paramName);
          values.put(paramName, parseDate(value));
        }
        case ARRAY_OF_DATE -> {
          // '1981-02-12' = ANY("mycolumn")
          clauses.add(":" + paramName + " = ANY(" + quote(column) + ")");
          values.put(paramName, parseDate(value));
        }
        case DATE_TIME -> {
          // "mycolumn" = '1981-02-12 19:00:00'
          clauses.add(quote(column) + " = :" + paramName);
          values.put(paramName, parseDateTime(value));
        }
        case ARRAY_OF_DATE_TIME -> {
          // '1981-02-12 19:00:00' = ANY("mycolumn")
          clauses.add(":" + paramName + " = ANY(" + quote(column) + ")");
          values.put(paramName, parseDateTime(value));
        }
        case NULL, EMPTY_ARRAY ->
        // results in a `where false` clause. These columns are nonsensical to filter on, as
        // they cannot contain anything. Would it be better to throw InvalidQueryException?
        clauses.add("false");
        case ARRAY_OF_RELATION -> {
          // 'mysearchterm' IN (select split_part(unnest, '/', 3) from unnest("mycolumn")
          /* values in the column will be of the form "terra-wds:/${targetType}/${targetId}".
             This SQL splits the values on "/", finds the third index in the split,
             and searches on that value.
          */
          clauses.add(
              ":"
                  + paramName
                  + " IN (select LOWER(split_part(unnest, '/', 3)) from unnest("
                  + quote(column)
                  + "))");
          values.put(paramName, value.toLowerCase());
        }
        case JSON -> {
          // "mycolumn" = '{"myjson":"stuff"}'::jsonb
          // validate json input
          parseJson(value);

          clauses.add(quote(column) + " = :" + paramName + "::jsonb");
          values.put(paramName, value);
        }
        case ARRAY_OF_JSON -> {
          // '{"myjson":"stuff"}'::jsonb = ANY("mycolumn")
          // validate json input
          parseJson(value);

          clauses.add(":" + paramName + "::jsonb = ANY(" + quote(column) + ")");
          values.put(paramName, value);
        }
        default ->
        // this shouldn't happen, since all datatypes are covered above. Leaving this in place
        // as a safety net in case we add datatypes
        throw new InvalidQueryException("Column specified in query is of an unsupported datatype");
      }

      return new WhereClausePart(clauses, values);
    } else {
      throw new InvalidQueryException();
    }
  }

  // parse string into LocalDate; throw InvalidQueryException if unparsable
  private LocalDate parseDate(String value) {
    try {
      return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (Exception e) {
      throw new InvalidQueryException(
          "Query value for date column must be a valid ISO date string");
    }
  }

  // parse string into LocalDateTime; throw InvalidQueryException if unparsable
  private LocalDateTime parseDateTime(String value) {
    try {
      return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    } catch (Exception e) {
      throw new InvalidQueryException(
          "Query value for datetime column must be a valid ISO datetime string");
    }
  }

  // parse string into boolean, supporting only "true" or "false" strings, case-insensitive.
  // throw InvalidQueryException if unparsable
  private boolean strictParseBoolean(String value) {
    // could use DataTypeInferer.isValidBoolean here instead, but that requires spring beans
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    throw new InvalidQueryException(
        "Query value for boolean column must be either 'true' or 'false'");
  }

  // parse string into Double; throw InvalidQueryException if unparsable
  private Double parseNumericValue(String value) {
    BigDecimal parsedNumber;
    try {
      parsedNumber = new BigDecimal(value);
    } catch (NumberFormatException nfe) {
      throw new InvalidQueryException("Query value for numeric column must be a number");
    }
    return parsedNumber.doubleValue();
  }

  private JsonNode parseJson(String value) {
    /* N.B. this is a default ObjectMapper, which is NOT the same as the ObjectMapper used
       throughout WDS and managed by Spring. Here, we don't care about consistency with JSON
       parsing elsewhere in WDS; we only care that the input is parseable. After validating, the
       value will be sent to Postgres for Postgres to parse internally, and it's _that_ parsing
       that matters.
    */
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readTree(value);
    } catch (JsonProcessingException e) {
      throw new InvalidQueryException("Query value for json column must be valid json");
    }
  }

  // validate the column on which we are filtering
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
