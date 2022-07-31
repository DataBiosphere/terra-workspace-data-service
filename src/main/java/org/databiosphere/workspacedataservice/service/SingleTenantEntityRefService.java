package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.databiosphere.workspacedataservice.service.EntityReferenceService.ENTITY_NAME_KEY;
import static org.databiosphere.workspacedataservice.service.EntityReferenceService.ENTITY_TYPE_KEY;

@Service
public class SingleTenantEntityRefService {

    public Set<SingleTenantEntityReference> attachRefValueToEntitiesAndFindReferenceColumns(List<Entity> entities) {
        Set<SingleTenantEntityReference> result = new HashSet<>();
        for (Entity entity : entities) {
            Map<String, Object> attributes = entity.getAttributes().getAttributes();
            for (String attr : attributes.keySet()) {
                if(attributes.get(attr) instanceof Map) {
                    Map map = (Map)attributes.get(attr);
                    if(map.size() != 2 || !map.containsKey(ENTITY_NAME_KEY) || !map.containsKey(ENTITY_TYPE_KEY)){
                        continue;
                    }
                    attributes.put(attr, map.get(ENTITY_NAME_KEY));
                    entity.setAttributes(new EntityAttributes(attributes));
                    result.add(new SingleTenantEntityReference(attr, new EntityType((String) map.get(ENTITY_TYPE_KEY))));
                }
            }
        }
        return result;
    }
}
