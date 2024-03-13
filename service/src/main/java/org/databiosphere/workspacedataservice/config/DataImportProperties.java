package org.databiosphere.workspacedataservice.config;

import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.lang.Nullable;

/** Properties that dictate how data import processes should behave. */
public class DataImportProperties {
  private RecordSinkMode batchWriteRecordSink;
  private String projectId;
  private String rawlsBucketName;
  private boolean succeedOnCompletion;
  private final Set<AllowedHost> defaultAllowedHosts =
      Set.of(
          new AllowedHost("storage.googleapis.com"),
          new AllowedHost("*.core.windows.net"),
          // S3 allows multiple URL formats
          // https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
          new AllowedHost("s3.amazonaws.com"), // path style legacy global endpoint
          new AllowedHost("*.s3.amazonaws.com") // virtual host style legacy global endpoint
          );
  private Set<AllowedHost> allowedHosts = Collections.emptySet();
  private boolean fileImportsAllowed = false;

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

  /**
   * Accepted sources for imported files. This includes configured sources as well as default /
   * always allowed sources (GCS buckets, Azure storage containers, and S3 buckets).
   */
  public Set<AllowedHost> getAllowedHosts() {
    return Sets.union(defaultAllowedHosts, allowedHosts);
  }

  public void setAllowedHosts(@Nullable String[] allowedHosts) {
    this.allowedHosts =
        allowedHosts == null
            ? Collections.emptySet()
            : Arrays.stream(allowedHosts).map(AllowedHost::new).collect(Collectors.toSet());
  }

  /** Whether files may be imported from file: URLs. This should only be enabled for tests. */
  public boolean fileImportsAllowed() {
    return fileImportsAllowed;
  }

  public void setAllowFileImports(boolean allowFileImports) {
    this.fileImportsAllowed = allowFileImports;
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

  public class AllowedHost {
    private String pattern;

    public AllowedHost(String hostPattern) {
      this.pattern = hostPattern;
    }

    public boolean matchesUrl(URI url) {
      if (pattern.startsWith("*")) {
        return url.getHost().endsWith(pattern.substring(1));
      } else {
        return url.getHost().equals(pattern);
      }
    }
  }
}
