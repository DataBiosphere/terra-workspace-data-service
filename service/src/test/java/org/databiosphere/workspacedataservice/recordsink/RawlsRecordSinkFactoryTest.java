package org.databiosphere.workspacedataservice.recordsink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext
@SpringBootTest
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
class RawlsRecordSinkFactoryTest extends TestBase {

  @Autowired RecordSinkFactory recordSinkFactory;

  @Test
  void throwOnNullJobId() {
    ImportDetails importDetails =
        new ImportDetails(
            null, () -> "email", UUID.randomUUID(), RawlsAttributePrefixer.PrefixStrategy.NONE);

    Exception e =
        assertThrows(
            NullPointerException.class, () -> recordSinkFactory.buildRecordSink(importDetails));

    assertThat(e.getMessage()).contains("ImportDetails.jobId");
  }

  @Test
  void throwOnNullUserEmailSupplier() {
    ImportDetails importDetails =
        new ImportDetails(
            UUID.randomUUID(), null, UUID.randomUUID(), RawlsAttributePrefixer.PrefixStrategy.NONE);

    Exception e =
        assertThrows(
            NullPointerException.class, () -> recordSinkFactory.buildRecordSink(importDetails));

    assertThat(e.getMessage()).contains("ImportDetails.userEmailSupplier");
  }

  @Test
  void doNotThrowOnNonNull() {
    ImportDetails importDetails =
        new ImportDetails(
            UUID.randomUUID(),
            () -> "email",
            UUID.randomUUID(),
            RawlsAttributePrefixer.PrefixStrategy.NONE);

    assertDoesNotThrow(() -> recordSinkFactory.buildRecordSink(importDetails));
  }
}
