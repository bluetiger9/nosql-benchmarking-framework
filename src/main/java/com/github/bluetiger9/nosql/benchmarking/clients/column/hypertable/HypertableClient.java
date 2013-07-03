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
package com.github.bluetiger9.nosql.benchmarking.clients.column.hypertable;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_COLUMN_FAMILY;
import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_HOST;
import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_PORT;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsReader;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;
import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.ScanSpec;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.column.AbstractColumnStoreClient;

public class HypertableClient extends AbstractColumnStoreClient {
    private static final String PROPERTY_NAMESPACE = "namespace";
    private static final String PROPERTY_BUFFER_SIZE = "bufferSize";
    private static final String PROPERTY_TABLE = "table";

    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "38080";
    private static final String DEFAULT_NAMESPACE = "test";
    private static final String DEFAULT_BUFFER_SIZE = "4096";
    private static final String DEFAULT_COLUMN_FAMILY = "test";
    private static final String DEFAULT_TABLE = "test";
    
    private final String host;
    private final int port;
    private final String namespace;
    private final String columnFamily;
    private final String table;
    private final int bufferSize;
    
    private ThriftClient client;
    private long namespaceId;
    
    public HypertableClient(Properties props) {
        super(props);
        this.host = properties.getProperty(PROPERTY_HOST, DEFAULT_HOST);
        this.port = Integer.parseInt(properties.getProperty(PROPERTY_PORT, DEFAULT_PORT));
        this.namespace = properties.getProperty(PROPERTY_NAMESPACE, DEFAULT_NAMESPACE);
        this.bufferSize = Integer.parseInt(properties.getProperty(PROPERTY_BUFFER_SIZE, DEFAULT_BUFFER_SIZE));
        this.columnFamily = properties.getProperty(PROPERTY_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY);
        this.table = properties.getProperty(PROPERTY_TABLE, DEFAULT_TABLE);
    }

    @Override
    public void connect() throws ClientException {
        try {
            this.client = ThriftClient.create(host, port);
            this.namespaceId = client.open_namespace(namespace);
        } catch (Exception e) {
            throw new ClientException("connection error", e);
        }
        
    }
    
    @Override
    public void disconnect() throws ClientException {
        try {
            client.namespace_close(namespaceId);
        } catch (Exception e) {
            throw new ClientException("disconnect error", e);
        }
    }
    
    @Override
    public void put(String key, Map<String, String> columnValues) throws ClientException {
        try {
            long mutator = client.mutator_open(namespaceId, table, 0, 0);
            final SerializedCellsWriter writer = new SerializedCellsWriter(bufferSize * columnValues.size(), true);
            for (Map.Entry<String, String> pair : columnValues.entrySet()) {
                final String column = pair.getKey();
                final String value = pair.getValue();
                writer.add(key, columnFamily, column, SerializedCellsFlag.AUTO_ASSIGN, ByteBuffer.wrap(Bytes.toBytes(value)));
            }
            client.mutator_set_cells_serialized(mutator, writer.buffer(), true);
            client.mutator_close(mutator);
        } catch (Exception e) {
            throw new ClientException("put error", e);
        }
    }

    @Override
    public Map<String, String> get(String key, List<String> columns) throws ClientException {
        try {
            final ScanSpec scanSpec = new ScanSpec();
            final RowInterval rowInterval = new RowInterval();
            rowInterval.setStart_inclusive(true);
            rowInterval.setStart_row(key);
            scanSpec.setVersions(1);
            scanSpec.setRow_limit(1);
            
            for (String column : columns) {
                scanSpec.addToColumns(columnFamily + ":" + column);
            }
            
            final SerializedCellsReader reader = new SerializedCellsReader(null);
            
            long scanner = client.scanner_open(namespaceId, table, scanSpec);
            
            final Map<String, String> result = new HashMap<>();
            do {
                reader.reset(client.scanner_get_cells_serialized(scanner));
                while (reader.next()) {
                    result.put(new String(reader.get_column_qualifier()), new String(reader.get_value()));
                }                
            } while (!reader.eos());            
            client.scanner_close(scanner);
            
            return result;
        } catch (Exception e) {
            throw new ClientException("read error", e);
        }
    }

    @Override
    public void delete(String key) throws ClientException {
        try {
            final Cell cell = new Cell();
            cell.key = new Key();
            cell.key.row = key;
            cell.key.flag = KeyFlag.DELETE_ROW;
            client.set_cell(namespaceId, table, cell);
        } catch (Exception e) {
            throw new ClientException("delete error");
        }
    }

    @Override
    public void delete(String key, String column) throws ClientException {
        try {
            final Cell cell = new Cell();
            cell.key = new Key();
            cell.key.row = key;
            cell.key.column_qualifier = column;
            cell.key.flag = KeyFlag.DELETE_CELL;
            client.set_cell(namespaceId, table, cell);
        } catch (Exception e) {
            throw new ClientException("delete error");
        }
    }


}
