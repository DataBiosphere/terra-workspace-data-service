package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

/** A scalar attribute - that is, an attribute that has a single value. */
public abstract class ScalarAttribute<T> implements Attribute {
  final T value;
  final Class<?> clazz;

  private static final Logger LOGGER = LoggerFactory.getLogger(ScalarAttribute.class);

  ScalarAttribute(T value) {
    this.value = value;
    this.clazz = this.value.getClass();
  }

  public static ScalarAttribute<?> create(@Nullable Object input) {
    if (input instanceof Map amap) {
      LOGGER.warn("Actual value: " + StringUtils.join(amap));
    }

    if (input == null) {
      return NullAttribute.INSTANCE;
    }
    if (input instanceof Boolean booleanInput) {
      return new BooleanAttribute(booleanInput);
    }
    if (input instanceof LocalDate dateInput) {
      return new DateAttribute(dateInput);
    }
    if (input instanceof LocalDateTime dateTimeInput) {
      return new DateTimeAttribute(dateTimeInput);
    }
    if (input instanceof Number numberInput) {
      return new NumberAttribute(numberInput);
    }
    // ---------- relations
    if (input instanceof RelationAttribute relationAttribute) {
      return relationAttribute;
    }
    if (RelationUtils.isRelationValue(input)) {
      return new RelationAttribute(
          RelationUtils.getTypeValue(input), RelationUtils.getRelationValue(input));
    }
    if (input instanceof Map<?, ?> mapInput
        && mapInput.keySet().equals(Set.of("targetType", "targetId"))) {
      return new RelationAttribute(
          RecordType.valueOf(mapInput.get("targetType").toString()),
          mapInput.get("targetId").toString());
    }
    // ---------- relations
    if (input instanceof String stringInput && isFileType(stringInput)) {
      return new FileAttribute(stringInput);
    }
    if (input instanceof String stringInput) {
      return new StringAttribute(stringInput);
    }

    LOGGER.warn("Actual scalar value class: " + input.getClass().getName());

    return new StringAttribute(input.toString());
  }

  // TODO: copied from DataTypeInferer!
  private static boolean isFileType(String possibleFile) {
    URI fileUri;
    try {
      fileUri = new URI(possibleFile);
      // Many non-URI strings will parse without exception but have no scheme or host
      if (fileUri.getScheme() == null || fileUri.getHost() == null) {
        return false;
      }
    } catch (URISyntaxException use) {
      return false;
    }
    // https://[].blob.core.windows.net/[] or drs://[]
    return fileUri.getScheme().equalsIgnoreCase("drs")
        || (fileUri.getScheme().equalsIgnoreCase("https")
            && fileUri.getHost().toLowerCase().endsWith(".blob.core.windows.net"));
  }

  public Class<?> getBaseType() {
    return this.clazz;
  }

  @Override
  public T getValue() {
    return this.value;
  }

  @Override
  public Object sqlValue() {
    return this.value;
  }

  @Override
  public String toString() {
    return this.value.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ScalarAttribute<?> other) {
      return this.value.equals(other.value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }
}
