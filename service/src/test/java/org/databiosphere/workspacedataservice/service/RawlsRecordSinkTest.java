package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class RawlsRecordSinkTest {

  @Test
  void getAttributeName_prependsPrefix() {
    var recordSink = RawlsRecordSink.withPrefix("prefix");
    RecordType recordType = RecordType.valueOf("widget");
    assertThat(recordSink.getAttributeName(recordType, /* name= */ "attrName"))
        .isEqualTo("prefix:attrName");
  }

  @Test
  void getAttributeName_renamesNameToIncludeRecordType() {
    var recordSink = RawlsRecordSink.withPrefix("prefix");
    RecordType recordType = RecordType.valueOf("widget");
    assertThat(recordSink.getAttributeName(recordType, /* name= */ "name"))
        .isEqualTo("prefix:widget_name");
  }
}
