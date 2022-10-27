package org.databiosphere.workspacedataservice.shared.model;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.util.Scanner;

public class WDSVersionInfo {

    private static final String VERSION_FILE_LOCATION = "VERSION.txt";

    public List<String> getWDSVersionInfo() throws FileNotFoundException {
        System.out.println(System.getProperty("user.dir"));
        File versionFile = new File(VERSION_FILE_LOCATION);
        Scanner versionScanner = new Scanner(versionFile);
        List<String> versionDetails = new ArrayList<>();
        while (versionScanner.hasNextLine()) {
            String versionDetail = versionScanner.nextLine();
            versionDetails.add(versionDetail);
        }
        return versionDetails;
    }

}