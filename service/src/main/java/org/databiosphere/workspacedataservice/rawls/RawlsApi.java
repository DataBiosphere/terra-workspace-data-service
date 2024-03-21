package org.databiosphere.workspacedataservice.rawls;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.PostExchange;

/**
 * Rawls API signatures. Use these to create http proxies for outbound requests via e.g. RestClient.
 */
public interface RawlsApi {

  /**
   * List snapshot references in a workspace.
   *
   * @param workspaceId target workspace
   * @param offset pagination offset
   * @param limit pagination limit
   * @return list of snapshot references in this workspace.
   */
  @GetExchange("/api/workspaces/{workspaceId}/snapshots/v2")
  SnapshotListResponse enumerateDataRepoSnapshotByWorkspaceId(
      @PathVariable UUID workspaceId, @RequestParam int offset, @RequestParam int limit);

  /**
   * Create a new snapshot reference in a workspace
   *
   * @param workspaceId target workspace
   * @param namedDataRepoSnapshot the snapshot reference to create
   * @return the created snapshot reference
   */
  @PostExchange("/api/workspaces/{workspaceId}/snapshots/v2")
  DataRepoSnapshotResource createDataRepoSnapshotByWorkspaceId(
      @PathVariable UUID workspaceId, @RequestBody NamedDataRepoSnapshot namedDataRepoSnapshot);
}
