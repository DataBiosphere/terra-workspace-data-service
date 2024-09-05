package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Spring Data repository for managing WorkspaceRecord objects in the sys_wds.workspace table.
 *
 * <p>By extending CrudRepository, Spring will autogenerate 10+ methods to save, find, count and
 * delete rows in the sys_wds.workspace table. See CrudRepository for those method signatures.
 *
 * <p>When you inject a WorkspaceRepository bean into some other class, such as WorkspaceService,
 * you can call those save/find/delete methods and they will just work. Spring generates a proxy
 * class which implements those methods, and that proxy is what gets injected.
 *
 * <p>This means you won't see any code that actually implements those methods. They just work.
 */
public interface WorkspaceRepository extends CrudRepository<WorkspaceRecord, WorkspaceId> {

  // custom method to save a workspace record
  @Transactional
  @Modifying
  @Query(
      "INSERT INTO sys_wds.workspace (workspace_id, data_table_type) VALUES (:workspaceId, :workspaceDataTableType) ON CONFLICT DO NOTHING")
  void saveWorkspaceRecord(WorkspaceId workspaceId, WorkspaceDataTableType workspaceDataTableType);
}
