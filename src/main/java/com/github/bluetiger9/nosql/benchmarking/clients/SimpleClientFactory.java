package com.github.bluetiger9.nosql.benchmarking.clients;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.ComponentFactory;

@SuppressWarnings("rawtypes")
public class SimpleClientFactory implements ClientFactory {

    private final Class<? extends DatabaseClient> clientClass;
    private final Properties properties;

    public SimpleClientFactory(Class<? extends DatabaseClient> clientClass, Properties clientProperties) {
        this.properties = clientProperties;
        this.clientClass = clientClass;
    }
    
    @Override
    public DatabaseClient createClient() {
        return ComponentFactory.constructFromProperties(clientClass, properties);
    }
}
