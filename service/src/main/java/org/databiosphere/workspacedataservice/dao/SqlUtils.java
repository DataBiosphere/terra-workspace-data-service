package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException.NameType.RECORD_TYPE;

import java.util.UUID;
import java.util.regex.Pattern;
import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/** A collection of methods useful for working with SQL. */
public class SqlUtils {

  private SqlUtils() {}

  private static final Pattern DISALLOWED_CHARS_PATTERN =
      Pattern.compile("[^a-z0-9\\-_ ]", Pattern.CASE_INSENSITIVE);

  private static boolean containsDisallowedSqlCharacter(String name) {
    return name == null || DISALLOWED_CHARS_PATTERN.matcher(name).find();
  }

  public static String validateSqlString(String name, InvalidNameException.NameType nameType) {
    if (containsDisallowedSqlCharacter(name)
        || name.startsWith(ReservedNames.RESERVED_NAME_PREFIX)) {
      throw new InvalidNameException(nameType);
    }
    return name;
  }

  public static String quote(String toQuote) {
    return "\"" + toQuote + "\"";
  }

  public static String getQualifiedTableName(RecordType recordType, UUID instanceId) {
    // N.B. recordType is sql-validated in its constructor, so we don't need it here
    return quote(instanceId.toString())
        + "."
        + quote(SqlUtils.validateSqlString(recordType.getName(), RECORD_TYPE));
  }
}
