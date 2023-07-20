package org.databiosphere.workspacedataservice.process;

import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalProcessLaunchTest {

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
    void runSimpleCommandWithEnvs() throws IOException {
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
        String output = new String(
                localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.OUT)
                        .readAllBytes()).trim();
        String error = new String(
                localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR)
                        .readAllBytes()).trim();

        // block until the child process exits
        int exitCode = localProcessLauncher.waitForTerminate();
        assertEquals(0, exitCode);
        assertThat(output).isEqualTo("Hello World");
        assertThat(error).isEmpty();
    }

    @Test
    void runSimpleCommandAlwaysError() {
        List<String> processCommand = new ArrayList<>();
        // run a command that not ran incorrectly (return exit code 2)
        processCommand.add("false");

        // launch the child process
        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(processCommand, null);

        // block until the child process exits
        int exitCode = localProcessLauncher.waitForTerminate();
        assertEquals(1, exitCode);
    }

    @Test
    void runSimpleCommandCauseError() {
        List<String> processCommand = new ArrayList<>();
        // run a command that not ran incorrectly (return exit code 2)
        processCommand.add("javac");
        processCommand.add("-h");

        // launch the child process
        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
        localProcessLauncher.launchProcess(processCommand, null);

        // block until the child process exits
        int exitCode = localProcessLauncher.waitForTerminate();
        assertEquals(2, exitCode);
    }

    @Test
    void runSimpleCommandCauseException() {
        List<String> processCommand = new ArrayList<>();
        // run a command that is not recognized and will cause an exception
        processCommand.add("echo $TEST");

        // launch the child process
        LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();

        assertThrows(LaunchProcessException.class,
                () -> localProcessLauncher.launchProcess(processCommand, null),
                "Error launching local process"
        );
    }
}
