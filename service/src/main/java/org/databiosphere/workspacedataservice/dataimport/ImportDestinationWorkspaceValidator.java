package org.databiosphere.workspacedataservice.dataimport;

import bio.terra.workspace.model.WorkspaceDescription;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class ImportDestinationWorkspaceValidator {
  private final ImportRequirementsFactory importRequirementsFactory;
  private final WorkspaceManagerDao wsmDao;

  public ImportDestinationWorkspaceValidator(
      ImportRequirementsFactory importRequirementsFactory, WorkspaceManagerDao wsmDao) {
    this.importRequirementsFactory = importRequirementsFactory;
    this.wsmDao = wsmDao;
  }

  public void validateDestinationWorkspace(
      ImportRequestServerModel importRequest, UUID destinationWorkspaceId) {
    ImportRequirements requirements =
        importRequirementsFactory.getRequirementsForImport(importRequest.getUrl());

    if (requirements.protectedDataPolicy()) {
      if (!checkWorkspaceHasProtectedDataPolicy(destinationWorkspaceId)) {
        throw new ValidationException(
            "Data from this source can only be imported into a protected workspace.");
      }
    }
  }

  private boolean checkWorkspaceHasProtectedDataPolicy(UUID workspaceId) {
    try {
      WorkspaceDescription workspace = wsmDao.getWorkspace(workspaceId);
      return workspace.getPolicies().stream()
          .anyMatch(
              wsmPolicyInput ->
                  wsmPolicyInput.getNamespace().equals("terra")
                      && wsmPolicyInput.getName().equals("protected-data"));
    } catch (WorkspaceManagerException e) {
      if (e.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
        return false;
      } else {
        throw e;
      }
    }
  }
}
