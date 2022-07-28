package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.EntityReferenceService.ENTITY_NAME_KEY;
import static org.databiosphere.workspacedataservice.service.EntityReferenceService.ENTITY_TYPE_KEY;

@Service
public class SingleTenantEntityRefService {

    public List<SingleTenantEntityReference> findEntityReferences(List<Entity> entities) {
        List<SingleTenantEntityReference> result = new ArrayList<>();
        for (Entity entity : entities) {
            Map<String, Object> attributes = entity.getAttributes().getAttributes();
            for (String attr : attributes.keySet()) {
                if(attributes.get(attr) instanceof Map) {
                    Map map = (Map)attributes.get(attr);
                    if(map.size() != 2 || (map.containsKey(ENTITY_NAME_KEY) && map.containsKey(ENTITY_TYPE_KEY)){
                        continue;
                    }
                    attributes.put(attr, map.get(ENTITY_NAME_KEY));
                    entity.setAttributes(new EntityAttributes(attributes));
                }
            }
            List<Map> referencedAttrs = entity.getAttributes().getAttributes().entrySet().stream().filter(e -> {
                        Object value = e.getValue();
                        if(!(value instanceof Map)) {
                            return false;
                        }
                        Map
                    }).map(Map.class::cast)
                    .filter(map -> map.size() == 2 && map.containsKey(ENTITY_TYPE_KEY) && map.containsKey(ENTITY_NAME_KEY)).collect(Collectors.toList());
            for (Map referencedAttr : referencedAttrs) {
                result.add(new SingleTenantEntityReference(entity.getName(), entity.getEntityType(),
                        new EntityType(referencedAttr.get(ENTITY_TYPE_KEY).toString()), new EntityId(referencedAttr.get(ENTITY_NAME_KEY).toString())));
            }
        }
        return result;
    }
}
