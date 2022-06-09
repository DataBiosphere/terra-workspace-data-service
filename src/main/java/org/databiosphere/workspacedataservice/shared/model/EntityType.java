package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;

public class EntityType {

    private String name;

    private List<Entity> entities;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Entity> getEntities() {
        return entities;
    }

    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }
}
