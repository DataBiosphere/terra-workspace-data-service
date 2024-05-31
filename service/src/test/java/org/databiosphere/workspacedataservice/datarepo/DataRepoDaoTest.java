package org.databiosphere.workspacedataservice.datarepo;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class DataRepoDaoTest extends TestBase {

  @Autowired DataRepoDao dataRepoDao;

  @Autowired RecordDao recordDao;

  @Autowired CollectionDao collectionDao;

  @MockBean DataRepoClientFactory mockDataRepoClientFactory;

  final RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);
  private static final UUID COLLECTION_UUID = randomUUID();
  private static final CollectionId COLLECTION_ID = CollectionId.of(COLLECTION_UUID);

  @BeforeEach
  void setUp() {
    given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
    if (!collectionDao.collectionSchemaExists(COLLECTION_ID)) {
      collectionDao.createSchema(COLLECTION_ID);
    }
  }

  @AfterEach
  void tearDown() {
    if (collectionDao.collectionSchemaExists(COLLECTION_ID)) {
      collectionDao.dropSchema(COLLECTION_UUID);
    }
  }

  @Test
  void testSnapshotReturned() throws ApiException {
    final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(randomUUID());
    given(mockRepositoryApi.retrieveSnapshot(any(), any())).willReturn(testSnapshot);
    assertEquals(testSnapshot, dataRepoDao.getSnapshot(testSnapshot.getId()));
    Mockito.clearInvocations(mockRepositoryApi);
  }

  @Test
  void testErrorThrown() throws ApiException {
    final int statusCode = HttpStatus.UNAUTHORIZED.value();
    given(mockRepositoryApi.retrieveSnapshot(any(), any()))
        .willThrow(new ApiException(statusCode, "Intentional error thrown for unit test"));
    UUID randomUuid = randomUUID();
    var exception =
        assertThrows(DataRepoException.class, () -> dataRepoDao.getSnapshot(randomUuid));
    assertEquals(statusCode, exception.getStatusCode().value());
    Mockito.clearInvocations(mockRepositoryApi);
  }
}
