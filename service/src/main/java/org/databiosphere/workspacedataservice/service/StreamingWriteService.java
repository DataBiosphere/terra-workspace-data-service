package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class StreamingWriteService {

    private final RecordDao recordDao;

    private final DataTypeInferer inferer = new DataTypeInferer();

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWriteService.class);

    public StreamingWriteService(RecordDao recordDao) {
        this.recordDao = recordDao;
    }

    public void consumeStream(InputStream is, int batchSize, UUID instanceId, RecordType recordType){
        try(StreamingWriteHandler streamingWriteHandler = new StreamingWriteHandler(is)){
            boolean isFirstBatch = true;
            Map<String, DataTypeMapping> schema = null;
            for(StreamingWriteHandler.WriteStreamInfo info = streamingWriteHandler.readRecords(batchSize); !info.getRecords().isEmpty(); info = streamingWriteHandler.readRecords(batchSize)){
                List<Record> records = info.getRecords();
                if(isFirstBatch){
                    schema = inferer.inferTypes(records);
                    isFirstBatch = false;
                    if(!recordDao.recordTypeExists(instanceId, recordType)){
                        recordDao.createRecordType(instanceId, schema, recordType, Collections.emptySet());
                    }
                }
                switch (info.getOperationType()){
                    case CREATE, UPDATE -> recordDao.batchUpsert(instanceId, recordType, records, schema);
                    case DELETE -> recordDao.batchDelete(instanceId, recordType, records);
                    case REPLACE -> recordDao.batchReplace(instanceId, recordType, records);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
