package org.databiosphere.workspacedataservice.config;

import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Properties that dictate how data import processes should behave. */
public class DataImportProperties {
  private static final Set<Pattern> DEFAULT_ALLOWED_HOSTS =
      Set.of(
          Pattern.compile("storage\\.googleapis\\.com"),
          Pattern.compile(".*\\.core\\.windows\\.net"),
          // S3 allows multiple URL formats
          // https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
          Pattern.compile("s3\\.amazonaws\\.com"), // path style legacy global endpoint
          Pattern.compile(".*\\.s3\\.amazonaws\\.com") // virtual host style legacy global endpoint
          );
  private RecordSinkMode batchWriteRecordSink;
  private String rawlsBucketName;
  private boolean succeedOnCompletion;
  private boolean enableTdrPermissionSync = false;

  private Set<Pattern> allowedHosts = emptySet();
  private Set<String> allowedSchemes = Set.of("https");
  private String statusUpdatesTopic;
  private String statusUpdatesSubscription;

  /** Where to write records after import, options are defined by {@link RecordSinkMode} */
  public RecordSinkMode getBatchWriteRecordSink() {
    return batchWriteRecordSink;
  }

  void setBatchWriteRecordSink(String batchWriteRecordSink) {
    this.batchWriteRecordSink = RecordSinkMode.fromValue(batchWriteRecordSink);
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

  /**
   * Permissions syncing on TDR import jobs is only enabled on the control plane.
   *
   * @see org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestQuartzJob
   * @return the configured value
   */
  public boolean isTdrPermissionSyncingEnabled() {
    return enableTdrPermissionSync;
  }

  public void setTdrPermissionSync(boolean enableTdrPermissionSync) {
    this.enableTdrPermissionSync = enableTdrPermissionSync;
  }

  /**
   * Accepted sources for imported files. This includes configured sources as well as default /
   * always allowed sources (GCS buckets, Azure storage containers, and S3 buckets).
   */
  public Set<Pattern> getAllowedHosts() {
    return Sets.union(DEFAULT_ALLOWED_HOSTS, allowedHosts);
  }

  public void setAllowedHosts(String[] allowedHosts) {
    this.allowedHosts = stream(allowedHosts).map(Pattern::compile).collect(Collectors.toSet());
  }

  public Set<String> getAllowedSchemes() {
    return allowedSchemes;
  }

  public void setAllowedSchemes(String[] allowedSchemes) {
    this.allowedSchemes = stream(allowedSchemes).collect(Collectors.toSet());
  }

  public String getStatusUpdatesTopic() {
    return statusUpdatesTopic;
  }

  public void setStatusUpdatesTopic(String statusUpdatesTopic) {
    this.statusUpdatesTopic = statusUpdatesTopic;
  }

  public String getStatusUpdatesSubscription() {
    return statusUpdatesSubscription;
  }

  public void setStatusUpdatesSubscription(String statusUpdatesSubscription) {
    this.statusUpdatesSubscription = statusUpdatesSubscription;
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
