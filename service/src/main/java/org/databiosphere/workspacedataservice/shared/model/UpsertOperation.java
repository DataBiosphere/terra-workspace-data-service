package org.databiosphere.workspacedataservice.shared.model;

public class UpsertOperation {

    private UpsertAction op;

    private String attributeName;

    private Object addUpdateAttribute;

    public UpsertOperation() {
    }

    public UpsertOperation(UpsertAction op, String attributeName) {
        this.op = op;
        this.attributeName = attributeName;
    }

    public UpsertOperation(UpsertAction op, String attributeName, Object addUpdateAttribute) {
        this.op = op;
        this.attributeName = attributeName;
        this.addUpdateAttribute = addUpdateAttribute;
    }

    public UpsertAction getOp() {
        return op;
    }

    public void setOp(UpsertAction op) {
        this.op = op;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public Object getAddUpdateAttribute() {
        return addUpdateAttribute;
    }

    public void setAddUpdateAttribute(Object addUpdateAttribute) {
        this.addUpdateAttribute = addUpdateAttribute;
    }
}
