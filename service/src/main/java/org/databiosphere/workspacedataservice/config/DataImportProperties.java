package org.databiosphere.workspacedataservice.config;

import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.lang.Nullable;

/** Properties that dictate how data import processes should behave. */
public class DataImportProperties {
  private String rawlsBucketName;
  private boolean succeedOnCompletion;
  private boolean enableTdrPermissionSync = false;

  private Set<Pattern> allowedHosts = emptySet();
  private String rawlsNotificationsTopic;
  private String statusUpdatesTopic;
  private String statusUpdatesSubscription;
  private List<ImportSourceConfig> sources;
  private boolean shouldAddImportMetadata = false;
  private boolean connectivityCheckEnabled = false;

  /** Where to write Rawls JSON files after import. */
  @Nullable
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

  public void setEnableTdrPermissionSync(boolean enableTdrPermissionSync) {
    this.enableTdrPermissionSync = enableTdrPermissionSync;
  }

  /**
   * Accepted sources for imported files. This includes configured sources as well as default /
   * always allowed sources (GCS buckets, Azure storage containers, and S3 buckets).
   */
  public Set<Pattern> getAllowedHosts() {
    return allowedHosts;
  }

  public void setAllowedHosts(String[] allowedHosts) {
    this.allowedHosts = stream(allowedHosts).map(Pattern::compile).collect(Collectors.toSet());
  }

  public String getRawlsNotificationsTopic() {
    return rawlsNotificationsTopic;
  }

  public void setRawlsNotificationsTopic(String rawlsNotificationsTopic) {
    this.rawlsNotificationsTopic = rawlsNotificationsTopic;
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

  public List<ImportSourceConfig> getSources() {
    return sources;
  }

  public void setSources(List<ImportSourceConfig> sources) {
    this.sources = sources;
  }

  /**
   * Should add import metadata to TDR imports? Currently, this only works for Rawls record sinks
   * (in the control plane).
   */
  public boolean shouldAddImportMetadata() {
    return shouldAddImportMetadata;
  }

  public void setAddImportMetadata(boolean shouldAddImportMetadata) {
    this.shouldAddImportMetadata = shouldAddImportMetadata;
  }

  public boolean isConnectivityCheckEnabled() {
    return connectivityCheckEnabled;
  }

  public void setConnectivityCheckEnabled(boolean connectivityCheckEnabled) {
    this.connectivityCheckEnabled = connectivityCheckEnabled;
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

  public record ImportSourceConfig(
      List<Pattern> urls, boolean requirePrivateWorkspace, boolean requireProtectedDataPolicy) {
    public boolean matchesUri(URI uri) {
      String uriString = uri.toString();
      return urls.stream().anyMatch(urlPattern -> urlPattern.matcher(uriString).find());
    }
  }
}
