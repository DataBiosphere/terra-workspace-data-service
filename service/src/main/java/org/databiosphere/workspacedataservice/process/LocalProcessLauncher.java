package org.databiosphere.workspacedataservice.process;

import org.springframework.stereotype.Component;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** This class provides utility methods for launching local child processes. */
@Component
public class LocalProcessLauncher {
    private Process process;

    public enum Output {
        OUT,
        ERROR
    }

    /**
<<<<<<< HEAD
     * LocalProcessLauncher will be used to run processes.
     */
    public LocalProcessLauncher() {}

    /**
=======
>>>>>>> main
     * Executes a command in a separate process from the current working directory (i.e. the same
     * place as this Java process is running).
     *
     * @param command the command and arguments to execute
     * @param envVars the environment variables to set or overwrite if already defined
     */
    public void launchProcess(List<String> command, Map<String, String> envVars) {
        launchProcess(command, envVars, null);
    }

    /**
     * Executes a command in a separate process from the given working directory, with the given
     * environment variables set beforehand.
     *
     * @param command the command and arguments to execute
     * @param envVars the environment variables to set or overwrite if already defined
     * @param workingDirectory the working directory to launch the process from
     */
    public InputStream launchProcess(
            List<String> command, Map<String, String> envVars, Path workingDirectory) {
        // build and run process from the specified working directory
        ProcessBuilder procBuilder = new ProcessBuilder(command);
        if (workingDirectory != null) {
            procBuilder.directory(workingDirectory.toFile());
        }
        if (envVars != null) {
            Map<String, String> procEnvVars = procBuilder.environment();
            procEnvVars.putAll(envVars);
        }

        try {
            process = procBuilder.start();
            return process.getInputStream();
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error launching local process", ioEx);
        }
    }

    /**
     * Method to stream the child process' output to a string.
     *
     * @param fromStream specifies which stream will be read from
     */
    public String getOutputForProcessFromStream(InputStream fromStream) {
        try (BufferedReader bufferedReader =
                     new BufferedReader(new InputStreamReader(fromStream, StandardCharsets.UTF_8))) {

            String line;
            StringBuilder processOutput = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                processOutput.append(line).append(System.lineSeparator());
            }

            return processOutput.toString().trim();
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error streaming output of child process", ioEx);
        }
    }

    /** Stream standard out/err from the child process to the CLI console.
     *
     *  @param type specifies which process stream to get data from
     * */
    public String getOutputForProcess(Output type) {
        if (type == Output.ERROR) {
            return getOutputForProcessFromStream(process.getErrorStream());
        }

        return getOutputForProcessFromStream(process.getInputStream());
    }

    /** Block until the child process terminates, then return its exit code. */
    public int waitForTerminate() {
        try {
            return process.waitFor();
        } catch (InterruptedException intEx) {
            Thread.currentThread().interrupt();
            throw new LaunchProcessException("Error waiting for child process to terminate", intEx);
        }
    }
}
