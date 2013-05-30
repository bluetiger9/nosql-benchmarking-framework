package com.github.bluetiger9.nosql.benchmarking;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.github.bluetiger9.nosql.benchmarking.runners.Runner;

public final class Main {
    private static final String PROPERTY_RUNNER = "runner";
    
    private static final String DEFAULT_RUNNER = "com.githb.bluetiger9.nosql.benchmarking.runners.BasicRunner";
    
    private static final Logger LOGGER = Logger.getLogger(Main.class);
    
    /**
     * The main method.
     * @param args
     */
    public static void main(String[] args) {
        LOGGER.info("Program started with arguments: " + Arrays.toString(args));
        
        if (args.length != 1) {
            System.out.println("Usage: Main MAIN_CONFIGURATION_FILE");
            LOGGER.fatal("Wrong number of arguments");
        }
        
        final Properties mainProperties = loadMainProperties(args[0]);
        
        createRunner(mainProperties);
    }

    /**
     * Loads the main configuration file.
     * @param mainConfigFile the configuration file
     * @return the loaded properties
     */
    private static Properties loadMainProperties(final String mainConfigFile) {
        Properties mainProperties = null;
        try {
            LOGGER.info("Loading the main configuration file: " + mainConfigFile);
            mainProperties = Util.loadProperties(mainConfigFile);
        } catch (IOException e) {
            LOGGER.fatal("Unable to read the main configuration file. Exiting", e);
            System.exit(-1);
        }
        
        LOGGER.info("Main properties: " + mainProperties.toString());
        return mainProperties;
    }

    /**
     * Loads the configured runner class and constructs the runner.
     * @param mainProperties the properties
     * @return the newly created runner
     */
    private static Runner createRunner(Properties mainProperties) {
        final String runnerClass = mainProperties.getProperty(PROPERTY_RUNNER, DEFAULT_RUNNER);
        
        Class<? extends Runner> runnerClazz = null;
        try {
            LOGGER.info("Loading the runner class: " + runnerClass);
            runnerClazz = ComponentFactory.loadClass(Runner.class, runnerClass);
        } catch (ClassNotFoundException e) {
            LOGGER.fatal("Failed to load the runner class. Exiting", e);
            System.exit(-1);
        }
        
        LOGGER.info("Runner class loaded succesfully. Creating the runner.");
        return ComponentFactory.constructFromProperties(Runner.class, runnerClazz, mainProperties);
    }
}
