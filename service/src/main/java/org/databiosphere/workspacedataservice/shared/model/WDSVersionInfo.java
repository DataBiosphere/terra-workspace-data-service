package org.databiosphere.workspacedataservice.shared.model;

public class WDSVersionInfo {

    public WDSVersionInfo() {
        String gitShortSha = getSemanticVersion();
        String semanticVersion = getGitShortSha();
    }

    private String getSemanticVersion() {
        String str = "hello";
        return str;
    }

    private String getGitShortSha() {
        String str = "hello";
        return str;
    }

}