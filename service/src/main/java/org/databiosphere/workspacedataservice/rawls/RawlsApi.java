package org.databiosphere.workspacedataservice.rawls;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.PatchExchange;
import org.springframework.web.service.annotation.PostExchange;

/**
 * Rawls API signatures. Use these to create http proxies for outbound requests via e.g. RestClient.
 */
public interface RawlsApi {

  /**
   * Get a single workspace's details, optionally filtered to only the specified fields.
   *
   * @param workspaceId target workspace
   * @param fields comma separated list of fields
   * @return workspace details
   */
  @GetExchange("/api/workspaces/id/{workspaceId}")
  RawlsWorkspaceDetails getWorkspaceDetails(
      @PathVariable UUID workspaceId, @RequestParam String fields);

  /**
   * Create a new snapshot reference in a workspace
   *
   * @param workspaceId target workspace
   * @param snapshotIds ids of the snapshots to reference
   * @return the created snapshot reference
   */
  @PostExchange("/api/workspaces/{workspaceId}/snapshots/v3")
  DataRepoSnapshotResource createSnapshotsByWorkspaceIdV3(
      @PathVariable UUID workspaceId, @RequestBody List<UUID> snapshotIds);

  @PatchExchange("/api/workspaces/v2/{workspaceNamespace}/{workspaceName}/authDomain")
  void addAuthDomainGroups(
      @PathVariable("workspaceNamespace") String workspaceNamespace,
      @PathVariable("workspaceName") String workspaceName,
      @RequestBody List<String> authDomainGroups);
}
