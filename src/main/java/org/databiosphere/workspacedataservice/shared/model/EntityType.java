package org.databiosphere.workspacedataservice.shared.model;

public class EntityType {

    public EntityType(String name) {
        this.name = name;
    }

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
