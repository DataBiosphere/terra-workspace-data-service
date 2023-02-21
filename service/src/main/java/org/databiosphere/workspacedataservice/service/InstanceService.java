package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@Service
public class InstanceService {

    private final RecordDao recordDao;

    public InstanceService(RecordDao recordDao) {
        this.recordDao = recordDao;
    }

    public List<UUID> listInstances(String version) {
        validateVersion(version);
        return recordDao.listInstanceSchemas();
    }

    @WriteTransaction
    public void createInstance(UUID instanceId, String version) {
        validateVersion(version);
        if (recordDao.instanceSchemaExists(instanceId)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "This instance already exists");
        }
        recordDao.createSchema(instanceId);
    }

    @WriteTransaction
    public void deleteInstance(UUID instanceId, String version) {
        validateVersion(version);
        validateInstance(instanceId);
        recordDao.dropSchema(instanceId);
    }

    public void validateInstance(UUID instanceId) {
        if (!recordDao.instanceSchemaExists(instanceId)) {
            throw new MissingObjectException("Instance");
        }
    }
}
