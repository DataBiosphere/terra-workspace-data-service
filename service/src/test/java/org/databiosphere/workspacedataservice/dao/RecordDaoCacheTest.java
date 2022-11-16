package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.AopTestUtils;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ContextConfiguration
@SpringBootTest
public class RecordDaoCacheTest {

    @Autowired
    private RecordDao recordDao;

    private RecordDao mock;
    private UUID instanceId;

    @EnableCaching
    @Configuration
    public static class CachingTestConfig {

        @Bean
        public RecordDao mockedRecordDao() {
            return mock(RecordDao.class);
        }

        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("primaryKeys");
        }

    }

    @BeforeEach
    void setUp() {
        mock = AopTestUtils.getTargetObject(recordDao);

        reset(mock);

        instanceId = UUID.randomUUID();
        when(mock.getRecordIdColumn(eq(RecordType.valueOf("foo")), eq(instanceId)))
                .thenReturn("sys_name");
        when(mock.getRecordIdColumn(eq(RecordType.valueOf("bar")), eq(instanceId)))
                .thenReturn("sys_name");
    }

    @Test
    void foo(){
        RecordType rt = RecordType.valueOf("foo");
        RecordType rtBar = RecordType.valueOf("bar");
        recordDao.getRecordIdColumn(rt, instanceId);
        recordDao.getRecordIdColumn(rt, instanceId);
        recordDao.getRecordIdColumn(rtBar, instanceId);
        //the second call should be cached and not increment invocations
        verify(mock, times(1)).getRecordIdColumn(rt, instanceId);
        verify(mock, times(1)).getRecordIdColumn(rtBar, instanceId);
        //this should evict the entry for rt+instance
        recordDao.createRecordType(instanceId, Collections.emptyMap(), rt, Collections.emptySet(), "blah");
        recordDao.getRecordIdColumn(rt, instanceId);
        //should still cach
        recordDao.getRecordIdColumn(rtBar, instanceId);
        //since the createRecordType call should evict invocations should tick up one
        verify(mock, times(2)).getRecordIdColumn(rt, instanceId);
        //should stay at 1 since it was never evicted
        verify(mock, times(1)).getRecordIdColumn(rtBar, instanceId);
    }
}
