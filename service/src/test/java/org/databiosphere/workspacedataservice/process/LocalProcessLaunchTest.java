package org.databiosphere.workspacedataservice.process;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalProcessLaunchTest {

    @Test
    void runSimpleCommand() {
        List<String> processCommand = new ArrayList<>();
        processCommand.add("ls");
        processCommand.add("-a");

        // launch the child process
        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(processCommand, null);

        // block until the child process exits
        int exitCode = localProcessLauncher.waitForTerminate();
        assertEquals(0, exitCode);
    }
}
