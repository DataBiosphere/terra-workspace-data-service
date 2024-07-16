package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.springframework.data.repository.CrudRepository;

public interface CollectionRepository extends CrudRepository<WdsCollection, UUID> {}
