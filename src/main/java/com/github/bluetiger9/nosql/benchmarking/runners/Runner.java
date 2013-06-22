package com.github.bluetiger9.nosql.benchmarking.runners;

import java.io.IOException;
import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.Component;
import com.github.bluetiger9.nosql.benchmarking.ComponentFactory;
import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.benchmarks.Benchmark;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientFactory;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;
import com.github.bluetiger9.nosql.benchmarking.clients.SimpleClientFactory;

@SuppressWarnings("rawtypes")
public class Runner extends Component {
    private static final String PROPERTY_BENCHMARK_CLASS = "benchmark.class";
    private static final String PROPERTY_BENCHMARK_PROPERTIES = "benchmark.properties";
    private static final String PROPERTY_CLIENT_CLASS = "client.class";
    private static final String PROPERTY_CLIENT_PROPERTIES = "client.properties";

    private final Class<? extends Benchmark> benchmarkClass;
    private final Class<? extends DatabaseClient> clientClass;
    private final Properties benchmarkProperties;
    private final Properties clientProperties;
    private final ClientFactory clientFactory;
    private final Benchmark benchmark;

    public Runner(Properties props) {
        super(props);

        this.benchmarkClass = loadBenchmarkClass();
        this.benchmarkProperties = loadBenchmarkProperties();

        this.clientClass = loadClientClass();
        this.clientProperties = loadClientProperties();
        
        addRuntimeProperties(benchmarkProperties);

        this.clientFactory = new SimpleClientFactory(clientClass, clientProperties);
        this.benchmark = ComponentFactory.constructFromProperties(Benchmark.class, benchmarkClass, benchmarkProperties);
    }
    
    @SuppressWarnings("unchecked")
    public void runBenchmark() {
        try {
            benchmark.run(clientFactory);
        } catch (ClassCastException e) {
            logger.error("Wrong client type");
            throw new RuntimeException(e);
        }
    }

    private Class<? extends Benchmark> loadBenchmarkClass() {
        try {
            final String benchmarkClass = Util.getMandatoryProperty(properties, PROPERTY_BENCHMARK_CLASS);
            return ComponentFactory.loadClass(Benchmark.class, benchmarkClass);
        } catch (ClassNotFoundException e) {
            logger.error("Benchmark class not found", e);
            throw new RuntimeException(e);
        }
    }

    private Class<? extends DatabaseClient> loadClientClass() {
        try {
            final String clientClass = Util.getMandatoryProperty(properties, PROPERTY_CLIENT_CLASS);
            return ComponentFactory.loadClass(DatabaseClient.class, clientClass);
        } catch (ClassNotFoundException e) {
            logger.error("Client class not found", e);
            throw new RuntimeException(e);
        }
    }

    private Properties loadClientProperties() {
        try {
            final String clientPropertiesFile = Util.getMandatoryProperty(properties, PROPERTY_CLIENT_PROPERTIES);
            return Util.loadProperties(clientPropertiesFile);
        } catch (IOException e) {
            logger.error("Failed to load the client properties", e);
            throw new RuntimeException(e);
        }
    }

    private Properties loadBenchmarkProperties() {
        try {
            final String benchmarkPropertiesFile = Util.getMandatoryProperty(properties, PROPERTY_BENCHMARK_PROPERTIES);
            return Util.loadProperties(benchmarkPropertiesFile);
        } catch (IOException e) {
            logger.error("Failed to load the benchmark properties", e);
            throw new RuntimeException(e);
        }
    }
    
    private void addRuntimeProperties(Properties benchmarkProperties) {
        properties.remove(PROPERTY_BENCHMARK_CLASS);
        properties.remove(PROPERTY_BENCHMARK_PROPERTIES);
        properties.remove(PROPERTY_CLIENT_CLASS);
        properties.remove(PROPERTY_CLIENT_PROPERTIES);
        
        // the remaining properties are runtime properties
        benchmarkProperties.putAll(properties);        
    }
}
