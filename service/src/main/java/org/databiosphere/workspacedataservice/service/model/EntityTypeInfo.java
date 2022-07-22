package org.databiosphere.workspacedataservice.service.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class EntityTypeInfo {

    private List<String> attributeNames;

    private int count;

    private String idName;

    @JsonIgnore
    private String name;

    public EntityTypeInfo(String name, List<String> attributeNames) {
        this.name = name;
        this.attributeNames = attributeNames;
        this.idName = name + "_id";
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public void setAttributeNames(List<String> attributeNames) {
        this.attributeNames = attributeNames;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIdName() {
        return idName;
    }

    public void setIdName(String idName) {
        this.idName = idName;
    }
}
