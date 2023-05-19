package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.AopTestUtils;

import java.util.UUID;

import static org.mockito.Mockito.*;

@ContextConfiguration
@DirtiesContext
@SpringBootTest
class RecordDaoCacheTest {

    @Autowired
    @Qualifier("test")
    private CachedQueryDao cqDao;

    @Autowired
    private RecordDao recordDao;

    private CachedQueryDao mock;
    private UUID instanceId;

    @EnableCaching
    @Configuration
    static class CachingTestConfig {

        @Bean
        @Qualifier("test")
        public CachedQueryDao mockedCqDao() {
            return mock(CachedQueryDao.class);
        }

        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager(ReservedNames.PRIMARY_KEY_COLUMN_CACHE);
        }

        @Bean
        public RecordDao mockedRecordDao(){
            return mock(RecordDao.class);
        }

    }

    @BeforeEach
    void setUp() {
        mock = AopTestUtils.getTargetObject(cqDao);

        reset(mock);

        instanceId = UUID.randomUUID();
        when(mock.getPrimaryKeyColumn(RecordType.valueOf("foo"), instanceId))
                .thenReturn("sys_name");
        when(mock.getPrimaryKeyColumn(RecordType.valueOf("bar"), instanceId))
                .thenReturn("sys_name");
    }

    @Test
    void verifyCaching(){
        RecordType rt = RecordType.valueOf("foo");
        RecordType rtBar = RecordType.valueOf("bar");
        cqDao.getPrimaryKeyColumn(rt, instanceId);
        cqDao.getPrimaryKeyColumn(rt, instanceId);
        cqDao.getPrimaryKeyColumn(rtBar, instanceId);
        //the second call should be cached and not increment invocations
        verify(mock, times(1)).getPrimaryKeyColumn(rt, instanceId);
        verify(mock, times(1)).getPrimaryKeyColumn(rtBar, instanceId);
        //this should evict the entry for rt+instance
        recordDao.deleteRecordType(instanceId,  rt);
        cqDao.getPrimaryKeyColumn(rt, instanceId);
        //should go back to caching
        cqDao.getPrimaryKeyColumn(rtBar, instanceId);
        cqDao.getPrimaryKeyColumn(rtBar, instanceId);
        //since the deleteRecordType call should evict invocations should tick up one
        verify(mock, times(2)).getPrimaryKeyColumn(rt, instanceId);
        //should stay at 1 since it was never evicted
        verify(mock, times(1)).getPrimaryKeyColumn(rtBar, instanceId);
    }
}
