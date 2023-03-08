package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@Service
public class InstanceService {

    private final RecordDao recordDao;
    private final SamDao samDao;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceService.class);

    public InstanceService(RecordDao recordDao, SamDao samDao) {
        this.recordDao = recordDao;
        this.samDao = samDao;
    }

    public List<UUID> listInstances(String version) {
        validateVersion(version);
        return recordDao.listInstanceSchemas();
    }

    @WriteTransaction
    public void createInstance(UUID instanceId, String version, Optional<UUID> workspaceId) {
        validateVersion(version);

        UUID samResourceId = instanceId; // id of "wds-instance" Sam resource we will create
        UUID samParentResourceId = workspaceId.orElse(instanceId); // id of "workspace" Sam resource to use as the parent of "wds-instance"

        // check that the current user has permission on the parent workspace
        boolean hasCreateInstancePermission = samDao.hasCreateInstancePermission(samParentResourceId);
        LOGGER.debug("hasCreateInstancePermission? {}", hasCreateInstancePermission);

        if (!hasCreateInstancePermission) {
            throw new AuthorizationException("Caller does not have permission to create instance.");
        }

        if (recordDao.instanceSchemaExists(instanceId)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "This instance already exists");
        }

        // create `wds-instance` resource in Sam, specifying workspace as parent
        samDao.createInstanceResource(samResourceId, samParentResourceId);
        // create instance schema in Postgres
        recordDao.createSchema(instanceId);
    }

    @WriteTransaction
    public void deleteInstance(UUID instanceId, String version) {
        validateVersion(version);
        validateInstance(instanceId);

        // check that the current user has permission to delete the Sam resource
        boolean hasDeleteInstancePermission = samDao.hasDeleteInstancePermission(instanceId);
        LOGGER.debug("hasDeleteInstancePermission? {}", hasDeleteInstancePermission);

        if (!hasDeleteInstancePermission) {
            throw new AuthorizationException("Caller does not have permission to delete instance.");
        }

        // delete instance schema in Postgres
        recordDao.dropSchema(instanceId);
        // delete `wds-instance` resource in Sam
        samDao.deleteInstanceResource(instanceId);
    }

    public void validateInstance(UUID instanceId) {
        if (!recordDao.instanceSchemaExists(instanceId)) {
            throw new MissingObjectException("Instance");
        }
    }
}
