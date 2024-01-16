package org.databiosphere.workspacedataservice.service.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecordTypeSchemaTest {
  private static final RecordType RECORD_TYPE = RecordType.valueOf("record");

  private static final String PRIMARY_KEY = "id";

  private RecordTypeSchema schema;

  @BeforeEach
  void setUp() {
    List<AttributeSchema> attributes =
        Arrays.asList(
            new AttributeSchema("id", "STRING", null),
            new AttributeSchema("attr1", "STRING", null));

    schema = new RecordTypeSchema(RECORD_TYPE, attributes, 0, PRIMARY_KEY);
  }

  @Test
  void testIsPrimaryKey() {
    assertTrue(schema.isPrimaryKey(PRIMARY_KEY));
    assertFalse(schema.isPrimaryKey("attr1"));
  }

  @Test
  void testContainsAttribute() {
    assertTrue(schema.containsAttribute("attr1"));
    assertFalse(schema.containsAttribute("doesNotExist"));
  }
}
