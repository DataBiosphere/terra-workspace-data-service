package org.databiosphere.workspacedataservice.config;

/** Properties that dictate how data import processes should behave. */
public class DataImportProperties {
  /** Dictates the sink where BatchWriteService should write records after import. */
  public enum RecordSinkMode {
    WDS("wds"),
    RAWLS("rawls");
    private final String value;

    RecordSinkMode(String value) {
      this.value = value;
    }

    static RecordSinkMode fromValue(String value) {
      for (RecordSinkMode mode : RecordSinkMode.values()) {
        if (mode.value.equals(value)) {
          return mode;
        }
      }
      throw new RuntimeException("Unknown RecordSinkMode value: %s".formatted(value));
    }
  }

  private RecordSinkMode batchWriteRecordSink;

  /** Where to write records after import, options are defined by {@link RecordSinkMode} */
  public RecordSinkMode getBatchWriteRecordSink() {
    return batchWriteRecordSink;
  }

  void setBatchWriteRecordSink(String batchWriteRecordSink) {
    this.batchWriteRecordSink = RecordSinkMode.fromValue(batchWriteRecordSink);
  }

  private String projectId;

  public String getGoogleProjectId() {
    return projectId;
  }

  private String rawlsBucketName;

  public String getGoogleBucketName() {
    return rawlsBucketName;
  }
}
