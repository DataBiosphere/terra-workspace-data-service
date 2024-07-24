package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;

/**
 * Spring Data repository for managing WdsCollection objects in the sys_wds.collection table.
 *
 * <p>By extending CrudRepository, Spring will autogenerate 10+ methods to save, find, count and
 * delete rows in the sys_wds.collection table. See CrudRepository for those method signatures.
 *
 * <p>When you inject a CollectionRepository bean into some other class, such as CollectionService,
 * you can call those save/find/delete methods and they will just work. Spring generates a proxy
 * class which implements those methods, and that proxy is what gets injected.
 *
 * <p>This means you won't see any code that actually implements those methods. They just work.
 */
public interface CollectionRepository extends CrudRepository<WdsCollection, UUID> {

  // custom method to list collections by workspace
  @Query("SELECT * FROM sys_wds.collection WHERE workspace_id = :workspaceId")
  Iterable<WdsCollection> findByWorkspace(WorkspaceId workspaceId);
}
