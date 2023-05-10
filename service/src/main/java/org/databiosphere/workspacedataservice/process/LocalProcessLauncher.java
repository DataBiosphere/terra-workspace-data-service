package org.databiosphere.workspacedataservice.process;

import org.springframework.stereotype.Component;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** This class provides utility methods for launching local child processes. */
@Component
public class LocalProcessLauncher {
    private Process process;

    public enum Output {
        OUT,
        ERROR
    }

    /**
     * LocalProcessLauncher will be used to run processes.
     */
    public LocalProcessLauncher() {}
    /**
     * Executes a command in a separate process from the current working directory (i.e. the same
     * place as this Java process is running).
     *
     * @param command the command and arguments to execute
     * @param envVars the environment variables to set or overwrite if already defined
     */
    public void launchProcess(List<String> command) throws Exception {
        launchProcess(command, "hello");
    }

    /**
     * Executes a command in a separate process from the given working directory, with the given
     * environment variables set beforehand.
     *
     * @param command the command and arguments to execute
     * @param envVars the environment variables to set or overwrite if already defined
     * @param workingDirectory the working directory to launch the process from
     */
    public InputStream launchProcess(List<String> command, String dbPassword) throws Exception {
        File file = new File("backup.sql");
        String concatenatedString = command.stream().collect(Collectors.joining(" "));
        System.out.println(concatenatedString);
        ProcessBuilder procBuilder = new ProcessBuilder(concatenatedString);
        procBuilder.environment().put("PGPASSWORD", dbPassword);
        try {
            Object lock = new Object();
            process = procBuilder.start();
            procBuilder.directory();
            synchronized (lock) {
                // Wait for the process to complete
                process.waitFor();
            }
            return process.getInputStream();
        } catch (IOException ioEx) {
            throw new LaunchProcessException("Error launching local process", ioEx);
        } catch (InterruptedException e) {
            throw new Exception(e);
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
