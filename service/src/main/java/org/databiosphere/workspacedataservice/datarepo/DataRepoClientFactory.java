package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;

public interface DataRepoClientFactory {

    RepositoryApi getRepositoryApi();

}
