package org.databiosphere.workspacedataservice.config;

/** Properties that dictate how data import processes should behave. */
public class DataImportProperties {
  private RecordSinkMode batchWriteRecordSink;
  private String projectId;
  private String rawlsBucketName;
  private boolean succeedOnCompletion;

  /** Where to write records after import, options are defined by {@link RecordSinkMode} */
  public RecordSinkMode getBatchWriteRecordSink() {
    return batchWriteRecordSink;
  }

  void setBatchWriteRecordSink(String batchWriteRecordSink) {
    this.batchWriteRecordSink = RecordSinkMode.fromValue(batchWriteRecordSink);
  }

  public String getProjectId() {
    return projectId;
  }

  void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getRawlsBucketName() {
    return rawlsBucketName;
  }

  void setRawlsBucketName(String rawlsBucketName) {
    this.rawlsBucketName = rawlsBucketName;
  }

  /**
   * Should Quartz-based jobs transition to SUCCEEDED when they complete internally in WDS? In the
   * control plane, where a logical "job" requires Rawls to receive and write data, the Quartz job
   * will complete well before Rawls writes data, so we should not mark the job as completed. Rawls
   * will send a message indicating when the logical job is complete.
   *
   * @see org.databiosphere.workspacedataservice.dataimport.pfb.PfbQuartzJob
   * @see org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestQuartzJob
   * @return the configured value
   */
  public boolean isSucceedOnCompletion() {
    return succeedOnCompletion;
  }

  public void setSucceedOnCompletion(boolean succeedOnCompletion) {
    this.succeedOnCompletion = succeedOnCompletion;
  }

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
}
