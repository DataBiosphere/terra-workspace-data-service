package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedata.model.Job;
import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;

import java.util.UUID;

public class InstanceInitializerBean {

    private final InstanceDao instanceDao;
    private final BackupDao backupDao;
    private final LeonardoDao leoDao;
    private final WorkspaceDataServiceDao wdsDao;
    private final CloneDao cloneDao;

    private final BackupRestoreService restoreService;

    @Value("${twds.instance.workspace-id}")
    private String workspaceId;

    @Value("${twds.instance.source-workspace-id}")
    private String sourceWorkspaceId;

    @Value("${twds.startup-token}")
    private String startupToken;

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializerBean.class);

    public InstanceInitializerBean(InstanceDao instanceDao, BackupDao backupDao, LeonardoDao leoDao, WorkspaceDataServiceDao wdsDao, CloneDao cloneDao, BackupRestoreService restoreService){
        this.instanceDao = instanceDao;
        this.backupDao = backupDao;
        this.leoDao = leoDao;
        this.wdsDao = wdsDao;
        this.cloneDao = cloneDao;
        this.restoreService = restoreService;
    }

    public boolean isInCloneMode(String sourceWorkspaceId) {
        if (StringUtils.isNotBlank(sourceWorkspaceId)){
            LOGGER.info("SourceWorkspaceId found, checking database");
            try {
                UUID.fromString(sourceWorkspaceId);
            } catch (IllegalArgumentException e){
                    LOGGER.warn("SourceWorkspaceId could not be parsed, unable to clone DB. Provided SourceWorkspaceId: {}.", sourceWorkspaceId);
                    return false;
            }

            if (sourceWorkspaceId.equals(workspaceId)) {
                LOGGER.warn("SourceWorkspaceId and current WorkspaceId can't be the same.");
                return false;
            }

            try {
                // TODO at this stage of cloning work (where only backup is getting generated), just checking if an instance schema already exists is sufficient
                // when the restore operation is added, it would be important to check if any record of restore state is present
                // it is also possible to check if backup was initiated and completed (since if it did, we dont need to request it again)
                // and can just kick off the restore
                return !instanceDao.instanceSchemaExists(UUID.fromString(workspaceId));
            } catch (IllegalArgumentException e) {
                LOGGER.warn("WorkspaceId could not be parsed, unable to clone DB. Provided default WorkspaceId: {}.", workspaceId);
                return false;
            }
        }
        LOGGER.info("No SourceWorkspaceId found, initializing default schema.");
        return false;
    }

    /*
    Cloning comes from the concept of copying an original (source) workspace data (from WDS data tables) into
    a newly created (destination) workspace. WDS at start up will always have a current WorkspaceId, which in the
    context of cloning will effectively be the destination. The SourceWorkspaceId will only be populated if the currently
    starting WDS was initiated via a clone operation and will contain the WorkspaceId of the original workspace where the cloning
    was triggered.
    */
    public void initCloneMode(){
        LOGGER.info("Starting in clone mode...");

        try {
            // first get source wds url based on source workspace id and the provided access token
            var sourceWdsEndpoint = leoDao.getWdsEndpointUrl(startupToken);

            LOGGER.info("Retrieved source wds endpoint url {}", sourceWdsEndpoint);

            // make the call to source wds to trigger back up, to do that set the source WDS dao endpoint
            // url to the url retrieved from Leo
            wdsDao.setWorkspaceDataServiceUrl(sourceWdsEndpoint);

            // this fileName will be used for the restore
            var backupFileName = "";
            // check if the current workspace has already sent a request for backup
            // if it did, no need to do it again
            // TODO can also check the status and decide if backup should be tried again
            if (!cloneDao.cloneExistsForWorkspace(UUID.fromString(sourceWorkspaceId))) {
                LOGGER.info("No backup exists, will initiate one.");

                // TODO since the backup api is not async, this will return once the backup finishes
                var backupResponse = wdsDao.triggerBackup(startupToken, UUID.fromString(workspaceId));
                var trackingId = UUID.fromString(backupResponse.getJobId());
                LOGGER.info("Create clone entry in WDS to track cloning process.");
                cloneDao.createCloneEntry(trackingId, UUID.fromString(sourceWorkspaceId));

                LOGGER.info("Check on backup status in source workspace with Job Id {}.", backupResponse.getJobId());
                var backupStatusResponse = wdsDao.checkBackupStatus(startupToken, UUID.fromString(backupResponse.getJobId()));
                if (backupStatusResponse.getStatus().equals(Job.StatusEnum.SUCCEEDED)) {
                    backupFileName = backupStatusResponse.getResult().getFilename();
                    cloneDao.updateCloneEntryStatus(UUID.fromString(sourceWorkspaceId), CloneStatus.BACKUPSUCCEEDED);
                }
                else {
                    LOGGER.error("An error occurred during clone mode - backup not complete.");
                    cloneDao.terminateCloneToError(trackingId, backupStatusResponse.getErrorMessage());
                }
            }

            //TODO do the restore
            LOGGER.info("Restore from the following path on the source workspace storage container: {}", backupFileName);
            var restoreResponse = restoreService.restoreAzureWDS("v0.2", backupFileName, startupToken);
            //LOGGER.info("How Restore went: {} {}", restoreResponse.getResult().getStatus(), restoreResponse.getResult().getErrorMessage());

            //restoreService.restoreAzureWDS("v0.2", backupFileName);

        }
        catch(Exception e) {
            LOGGER.error("An error occurred during clone mode. Will start with empty default instance schema. Error: {}", e.toString());
        }
    }

    public void initializeInstance() {
        LOGGER.info("Default workspace id loaded as {}.", workspaceId);
        if (isInCloneMode(sourceWorkspaceId)) {
            LOGGER.info("Source workspace id loaded as {}.", sourceWorkspaceId);
            initCloneMode();
        }

        //TODO Wrap this in an else once restore for cloning is implemented (currently only backup is being kicked off)
        initializeDefaultInstance();
    }

    public void initializeDefaultInstance() {

        try {
            UUID instanceId = UUID.fromString(workspaceId);

            if (!instanceDao.instanceSchemaExists(instanceId)) {
                instanceDao.createSchema(instanceId);
                LOGGER.info("Creating default schema id succeeded for workspaceId {}.", workspaceId);
            } else {
                LOGGER.debug("Default schema for workspaceId {} already exists; skipping creation.", workspaceId);
            }

        } catch (IllegalArgumentException e) {
            LOGGER.warn("Workspace id could not be parsed, a default schema won't be created. Provided id: {}.", workspaceId);
        } catch (DataAccessException e) {
            LOGGER.error("Failed to create default schema id for workspaceId {}.", workspaceId);
        }
    }
}
