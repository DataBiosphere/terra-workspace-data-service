package org.databiosphere.workspacedataservice.process;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
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

    @Test
    void runSimpleCommandWithEnvs() {
        List<String> processCommand = new ArrayList<>();
        Map<String, String> envVars = new HashMap<>();
        envVars.put("TEST_VARIABLE", "Hello World");
        processCommand.add("bash");
        processCommand.add("-ce");
        processCommand.add("echo $TEST_VARIABLE");

        // launch the child process
        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(processCommand, envVars);

        // stream the output to stdout/err
        String output = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.OUT);
        String error = localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);

        // block until the child process exits
        int exitCode = localProcessLauncher.waitForTerminate();
        assertEquals(0, exitCode);
        assertThat(output).isEqualTo("Hello World");
        assertThat(error).isEmpty();
    }
}
