/*
 * Copyright (c) 2013 Attila Tőkés. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.github.bluetiger9.nosql.benchmarking.clients.column.cassandra;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_COLUMN_FAMILY;
import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_HOST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.column.AbstractColumnStoreClient;

public class CassandraClient extends AbstractColumnStoreClient {
    private static final String PROPERTY_CLUSTER = "cluster";
    private static final String PROPERTY_KEYSPACE = "keyspace";

    private static final String DEFAULT_CLUSTER = "test-cluster";
    private static final String DEFAULT_HOST = "localhost:9160";
    private static final String DEFAULT_KEYSPACE = "test";
    private static final String DEFAULT_COLUMN_FAMILY = "test";

    private final String clusterName;
    private final String host;
    private final String keyspaceName;
    private final String columnFamily;

    private Cluster cluster;
    private Keyspace keyspace;
    private ThriftColumnFamilyTemplate<String, String> columnFamilyTemplate;

    public CassandraClient(Properties props) {
        super(props);
        this.clusterName = properties.getProperty(PROPERTY_CLUSTER, DEFAULT_CLUSTER);
        this.host = properties.getProperty(PROPERTY_HOST, DEFAULT_HOST);
        this.keyspaceName = properties.getProperty(PROPERTY_KEYSPACE, DEFAULT_KEYSPACE);
        this.columnFamily = properties.getProperty(PROPERTY_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY);
    }

    @Override
    public void connect() throws ClientException {
        cluster = HFactory.getOrCreateCluster(clusterName, host);
        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspace,
                columnFamily, StringSerializer.get(), StringSerializer.get());
    }

    @Override
    public void disconnect() throws ClientException {
        //cluster.getConnectionManager().shutdown();
    }

    @Override
    public void put(String key, Map<String, String> columnValues) throws ClientException {
        final ColumnFamilyUpdater<String, String> updater = columnFamilyTemplate.createUpdater(key);
        for (Map.Entry<String, String> pair : columnValues.entrySet()) {
            final String column = pair.getKey();
            final String value = pair.getValue();
            updater.setString(column, value);
        }
        columnFamilyTemplate.update(updater);
    }

    @Override
    public Map<String, String> get(String key, List<String> columns) throws ClientException {
        final ColumnFamilyResult<String, String> queryResult = columnFamilyTemplate.queryColumns(key, columns);
        final Map<String, String> result = new HashMap<>();
        for (String column : columns) {
            result.put(column, queryResult.getString(column));
        }
        return result;
    }

    @Override
    public void delete(String key) {
        columnFamilyTemplate.deleteRow(key);
    }

    @Override
    public void delete(String key, String column) {
        columnFamilyTemplate.deleteColumn(key, column);
    }
}
