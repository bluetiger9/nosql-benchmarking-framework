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
package com.github.bluetiger9.nosql.benchmarking.clients.column.hbase;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_COLUMN_FAMILY;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.column.AbstractColumnStoreClient;

public class HBaseClient extends AbstractColumnStoreClient {
    private static final String PROPERTY_TABLE = "table";
    private static final String PROPERTY_ZOOKEEPER_HOST = "zookeeperHost";
    
    private static final String DEFAULT_TABLE = "test";
    private static final String DEFAULT_COLUMN_FAMILY = "test";
    private static final String DEFAULT_ZOOKEEPER_HOST = "localhost";

    private final String tableName;
    private final byte[] columnFamily;
    private final String zookeeperHost;
    
    private Configuration config;
    private HTable table;
    
    public HBaseClient(Properties props) {
        super(props);
        this.tableName = properties.getProperty(PROPERTY_TABLE, DEFAULT_TABLE);
        this.columnFamily = Bytes.toBytes(properties.getProperty(PROPERTY_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY));
        this.zookeeperHost = properties.getProperty(PROPERTY_ZOOKEEPER_HOST, DEFAULT_ZOOKEEPER_HOST);
    }
    
    @Override
    public void connect() throws ClientException {
        try {
            this.config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", zookeeperHost);
            this.table = new HTable(config, tableName);
        } catch (IOException e) {
            throw new ClientException("Connection error.", e);
        }                
    }

    @Override
    public void disconnect() throws ClientException {
        // TODO Auto-generated method stub

    }

    @Override
    public void put(String key, Map<String, String> columnValues) throws ClientException {
        final Put put = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, String> pair : columnValues.entrySet()) {
            final String column = pair.getKey();
            final String value = pair.getValue();
            put.add(columnFamily, Bytes.toBytes(column), Bytes.toBytes(value));
        }
        
        try {
            table.put(put);
        } catch (IOException e) {
            throw new ClientException("put error", e);
        }
    }

    @Override
    public Map<String, String> get(String key, List<String> columns) throws ClientException {
        final Get get = new Get(Bytes.toBytes(key));
        for (String column : columns) {
            get.addColumn(columnFamily, Bytes.toBytes(column));
        }
        
        try {
            final Result queryResult = table.get(get);
            final Map<String, String> result = new HashMap<String, String>();
            for (String column : columns) {
                final String value = Bytes.toString(queryResult.getValue(columnFamily, Bytes.toBytes(column)));
                result.put(column, value);
            }
            return result;
        } catch (IOException e) {
            throw new ClientException("get error", e);
        }
    }

    @Override
    public void delete(String key) throws ClientException {
        try {
            table.delete(new Delete(Bytes.toBytes(key)));
        } catch (IOException e) {
            throw new ClientException("delete error", e);
        }
    }

    @Override
    public void delete(String key, String column) throws ClientException {
        try {
            final Delete delete = new Delete(Bytes.toBytes(key));
            delete.deleteColumn(columnFamily, Bytes.toBytes(column));
            table.delete(delete);
        } catch (IOException e) {
            throw new ClientException("delete error", e);
        }
    }
}
