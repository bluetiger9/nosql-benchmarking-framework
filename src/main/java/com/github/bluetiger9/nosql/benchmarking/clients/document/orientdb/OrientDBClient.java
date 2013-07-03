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
package com.github.bluetiger9.nosql.benchmarking.clients.document.orientdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.document.AbstractDocumentStoreClient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OrientDBClient extends AbstractDocumentStoreClient {
    private static final String PROPERTY_URL = "url";
    private static final String PROPERTY_USER = "user";
    private static final String PROPERTY_PASSWORD = "password";
    
    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "admin";
    
    private static final String DOCUMENT_CLASS = "benchmark";

    private final String url;
    private final String username;
    private final String password;
    
    private ODatabaseDocumentTx client;
    private ODictionary<ORecordInternal<?>> dictionary;
    
    public OrientDBClient(Properties props) {
        super(props);
        this.url = Util.getMandatoryProperty(properties, PROPERTY_URL);
        this.username = properties.getProperty(PROPERTY_USER, DEFAULT_USER);
        this.password = properties.getProperty(PROPERTY_PASSWORD, DEFAULT_PASSWORD);        
    }

    @Override
    public void connect() throws ClientException {
        client = new ODatabaseDocumentTx(url).open(username, password);
        dictionary = client.getMetadata().getIndexManager().getDictionary();
        if ("true".equals(properties.getProperty("declareInsertIntent"))) {
            logger.info("Using insert intent");
            client.declareIntent(new OIntentMassiveInsert());
        }
    }
    
    @Override
    public void disconnect() throws ClientException {
        client.close();
    }
    
    @Override
    public void insert(String key, Map<String, String> attributes) throws ClientException {
        final ODocument document = client.newInstance(DOCUMENT_CLASS);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            document.field(entry.getKey(), entry.getValue());
        }
        document.save();
        dictionary.put(key, document);
    }

    @Override
    public Map<String, String> get(String key) throws ClientException {
        final ODocument document = dictionary.get(key);        
        if (document == null)
            return null;
        
        final Map<String, String> result = new HashMap<String, String>();
        for (final String field : document.fieldNames()) {
            result.put(field, document.field(field).toString());
        }
        return result;
    }

    @Override
    public void update(String key, Map<String, String> attributes) throws ClientException {
        final ODocument document = dictionary.get(key);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            document.field(entry.getKey(), entry.getValue());
        }
        document.save();        
    }

    @Override
    public void delete(String key) throws ClientException {
        dictionary.remove(key);
    }


}
