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
package com.github.bluetiger9.nosql.benchmarking.clients.graph;

import java.util.Map;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public interface GraphStoreClient extends DatabaseClient {

    void insertNode(String key, Map<String, String> attributes) throws ClientException;
    
    void insertEdge(String key1, String key2, Map<String, String> attributes) throws ClientException;
    
    Map<String, String> getNode(String key);
    
    Map<String, String> getEdge(String key1, String key2);
    
    void deleteNode(String key) throws ClientException;
    
    void deleteEdge(String key1, String key2) throws ClientException;
}
