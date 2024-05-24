package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;

/**
 * An array attribute - that is, an attribute that can have multiple values, all of which are the
 * same datatype.
 */
public abstract class ArrayAttribute<T extends ScalarAttribute<?>> implements Attribute {
  final List<T> value;

  protected ArrayAttribute(List<T> value) {
    this.value = value;
  }

  public static ArrayAttribute<?> create(List<?> value) {
    List<? extends ScalarAttribute<?>> converted =
        value.stream().map(ScalarAttribute::create).toList();

    // discovered types
    List<? extends Class<?>> discoveredTypes =
        converted.stream().map(x -> x.getClass()).distinct().toList();

    if (discoveredTypes.size() == 1) {
      Class<?> singleType = discoveredTypes.get(0);
      return switch (singleType.getSimpleName()) {
        case "BooleanAttribute" -> new BooleanArrayAttribute(
            converted.stream().map(x -> (BooleanAttribute) x).toList());
        case "DateAttribute" -> new DateArrayAttribute(
            converted.stream().map(x -> (DateAttribute) x).toList());
        case "DateTimeAttribute" -> new DateTimeArrayAttribute(
            converted.stream().map(x -> (DateTimeAttribute) x).toList());
        case "FileAttribute" -> new FileArrayAttribute(
            converted.stream().map(x -> (FileAttribute) x).toList());
        case "JsonAttribute" -> new JsonArrayAttribute(
            converted.stream().map(x -> (JsonAttribute) x).toList());
        case "NumberAttribute" -> new NumberArrayAttribute(
            converted.stream().map(x -> (NumberAttribute) x).toList());
        case "RelationAttribute" -> new RelationArrayAttribute(
            converted.stream().map(x -> (RelationAttribute) x).toList());
        case "StringAttribute" -> new StringArrayAttribute(
            converted.stream().map(x -> (StringAttribute) x).toList());
        default -> throw new RuntimeException("Unknown array type: " + singleType.getSimpleName());
      };
      // return array attribute of this type
    } else {
      // TODO AJ-858: don't just stringify everything
      List<StringAttribute> defaultToStrings =
          value.stream().map(x -> new StringAttribute(x.toString())).toList();
      return new StringArrayAttribute(defaultToStrings);

      // inspect for known-good combinations:
      //  - anything plus NullAttribute
      //  - file and string
      //  - relation and string
      //  - date and datetime?
      // return array of json
    }
  }

  @Override
  public List<T> getValue() {
    return this.value;
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ArrayAttribute<?> other) {
      return this.value.equals(other.value);
    }
    return false;
  }

  @Override
  public String toString() {
    return this.value.toString();
  }

  @Override
  public Object sqlValue() {
    // an array of the underlying sql values of each attribute in the list
    return this.value.stream().map(x -> x.sqlValue()).toArray();
  }

  @Override
  public Class<?> getBaseType() {
    return String.class;
  }
}
