package org.databiosphere.workspacedataservice.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.stream.Collectors;

@Component
public class AppStartPropertiesPrinter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppStartPropertiesPrinter.class);
    private final Environment environment;
    private static final String PROPERTIES_FILENAME = "application.properties";

    @Value("${twds.dump-properties}")
    private boolean dumpProperties;

    public AppStartPropertiesPrinter(Environment environment) {
        this.environment = environment;
    }

    @PostConstruct
    public void init() {
        if (dumpProperties) {
            ConfigurableEnvironment env = (ConfigurableEnvironment) environment;
            String summary = String.format(
                    "Dumping properties from %s, disable with twds.dump-properties=false", PROPERTIES_FILENAME);
            String indentedNewline = "\n\t";
            String spacer = indentedNewline + "---------------------------------------" + indentedNewline;
            String logMessage = summary +
                    spacer +
                    env.getPropertySources()
                            .stream()
                            .filter(ps -> ps instanceof MapPropertySource && ps.getName().contains(PROPERTIES_FILENAME))
                            .map(ps -> ((MapPropertySource) ps).getSource().keySet())
                            .flatMap(Collection::stream)
                            .distinct()
                            .sorted()
                            .map(key -> String.format("%s=%s", key, env.getProperty(key)))
                            .collect(Collectors.joining(indentedNewline)) +
                    spacer;
            LOGGER.info(logMessage);
        }
    }
}