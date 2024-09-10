package org.databiosphere.workspacedataservice.shared.model;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;
import org.databiosphere.workspacedataservice.dao.SqlUtils;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;

public class RecordType {

  private final String name;

  private RecordType(String name) {
    this.name = name;
    validate();
  }

  /*
   * the name "valueOf()" for this factory method is required since the
   * constructor is private. Spring uses valueOf() when extracting inbound
   *
   * @PathVariable arguments - which are Strings - into this class.
   */
  @JsonCreator
  public static RecordType valueOf(String recordTypeName) {
    return new RecordType(recordTypeName);
  }

  /*
   * N.B. we have a separate getName() method, even though getName() and
   * toString() are currently equivalent. In IntelliJ, it's much easier to find
   * usages of getName() than toString(), since toString() is an override.
   */
  @JsonValue
  public String getName() {
    return name;
  }

  public void validate() {
    if (name.startsWith(RESERVED_NAME_PREFIX)) {
      throw new InvalidNameException(InvalidNameException.NameType.RECORD_TYPE);
    }
    if (name.length() > 63) {
      throw new InvalidNameException(InvalidNameException.NameType.RECORD_TYPE);
    }
    SqlUtils.validateSqlString(name, InvalidNameException.NameType.RECORD_TYPE);
  }

  /*
   * returning the raw string value allows RecordType to be used directly as an
   * argument to url templates, e.g. within unit tests
   */
  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RecordType that = (RecordType) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
