package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;

public class EntityQueryResult {

    private EntityQueryParameters parameters;

    private EntityQueryResultMetadata resultMetadata;

    private List<Entity> results;

    public EntityQueryResult(EntityQueryParameters parameters, EntityQueryResultMetadata resultMetadata, List<Entity> results) {
        this.parameters = parameters;
        this.resultMetadata = resultMetadata;
        this.results = results;
    }

    public EntityQueryResult() {
    }

    public EntityQueryParameters getParameters() {
        return parameters;
    }

    public void setParameters(EntityQueryParameters parameters) {
        this.parameters = parameters;
    }

    public EntityQueryResultMetadata getResultMetadata() {
        return resultMetadata;
    }

    public void setResultMetadata(EntityQueryResultMetadata resultMetadata) {
        this.resultMetadata = resultMetadata;
    }

    public List<Entity> getResults() {
        return results;
    }

    public void setResults(List<Entity> results) {
        this.results = results;
    }

}
