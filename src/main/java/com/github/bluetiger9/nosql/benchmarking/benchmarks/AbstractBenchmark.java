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
package com.github.bluetiger9.nosql.benchmarking.benchmarks;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.Component;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientFactory;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public abstract class AbstractBenchmark<ClientType extends DatabaseClient> extends Component implements
        Benchmark<ClientType> {

    protected ClientFactory<? extends ClientType> clientFactory;
    
    public AbstractBenchmark(Properties props) {
        super(props);
    }
    
    public void initBenchmark(ClientFactory<? extends ClientType> clientFactory) {
        this.clientFactory = clientFactory;
    }
}
