package org.databiosphere.workspacedataservice.shared.model.attributes;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class JsonArrayAttribute extends ArrayAttribute<JsonAttribute> {
  JsonArrayAttribute(List<JsonAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_JSON;
  }

  @Override
  public String[] sqlValue() {
    return this.value.stream().map(JsonAttribute::sqlValue).toArray(String[]::new);
  }

  @Override
  public List<JsonNode> getValue() {
    return this.value.stream().map(JsonAttribute::getValue).toList();
  }
}
