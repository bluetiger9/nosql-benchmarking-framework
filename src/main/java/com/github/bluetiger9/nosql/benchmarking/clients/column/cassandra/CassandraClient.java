package com.github.bluetiger9.nosql.benchmarking.clients.column.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.column.AbstractColumnStoreClient;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.*;

public class CassandraClient extends AbstractColumnStoreClient {
    private static final String PROPERTY_CLUSTER = "cluster";
    private static final String PROPERTY_KEYSPACE = "keyspace";

    private static final String DEFAULT_CLUSTER = "test-cluster";
    private static final String DEFAULT_HOST = "localhost:9160";
    private static final String DEFAULT_KEYSPACE = "test";

    private final String clusterName;
    private final String host;
    private final String keyspaceName;

    private Cluster cluster;
    private Keyspace keyspace;
    
    private final Map<String, ColumnFamilyTemplate<String, String>> templates;

    public CassandraClient(Properties props) {
        super(props);
        this.clusterName = properties.getProperty(PROPERTY_CLUSTER, DEFAULT_CLUSTER);
        this.host = properties.getProperty(PROPERTY_HOST, DEFAULT_HOST);
        this.keyspaceName = properties.getProperty(PROPERTY_KEYSPACE, DEFAULT_KEYSPACE);
        this.templates = new HashMap<>();
    }

    @Override
    public void connect() throws ClientException {
        cluster = HFactory.getOrCreateCluster(clusterName, host);
        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
    }

    @Override
    public void disconnect() throws ClientException {
    }

    @Override
    public void insert(String key, String columnFamily, String column, String value) throws ClientException {
        final ColumnFamilyTemplate<String, String> template = getTemplate(columnFamily);
        final ColumnFamilyUpdater<String, String> updater = template.createUpdater(key);
        updater.setString(column, value);
        template.update(updater);
    }

    @Override
    public String get(String key, String columnFamily, String column) throws ClientException {
        return getTemplate(columnFamily).queryColumns(key).getString(column);
    }
    
    private ColumnFamilyTemplate<String, String> getTemplate(String columnFamily) {
        if (!templates.containsKey(columnFamily)) {
            templates.put(columnFamily, new ThriftColumnFamilyTemplate<String, String>(keyspace,
                    columnFamily, StringSerializer.get(), StringSerializer.get()));
        }
        return templates.get(columnFamily);
    }
}
