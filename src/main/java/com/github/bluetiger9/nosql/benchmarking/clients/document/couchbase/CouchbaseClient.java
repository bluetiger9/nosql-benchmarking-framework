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
package com.github.bluetiger9.nosql.benchmarking.clients.document.couchbase;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.document.DocumentStoreClient;
import com.mongodb.util.JSON;

public class CouchbaseClient extends com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.couchbase.CouchbaseClient implements DocumentStoreClient {

    public CouchbaseClient(Properties props) {
        super(props);
    }

    @Override
    public void insert(String key, Map<String, String> attributes) throws ClientException {
        super.insert(key, JSON.serialize(attributes));
    }

    @Override
    public Map<String, String> get(String key) throws ClientException {
        final String json = (String) super.get(key);
        if (json == null)
            return null;
        
        @SuppressWarnings("unchecked")
        final Map<String, String> document = (Map<String, String>) JSON.parse(json);
        final Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : document.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    public void update(String key, Map<String, String> attributes) throws ClientException {
        final Map<String, String> document = get(key);
        document.putAll(attributes);
        insert(key, document);
    }

    @Override
    public void delete(String key) throws ClientException {
        super.delete(key);
    }

}
