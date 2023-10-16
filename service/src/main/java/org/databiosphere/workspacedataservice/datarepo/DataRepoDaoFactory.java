package org.databiosphere.workspacedataservice.datarepo;

public interface DataRepoDaoFactory {
  DataRepoDao getDao(String authToken);

  DataRepoDao.ClientFactory getClientFactory(String authToken);
}
