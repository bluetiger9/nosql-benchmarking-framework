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
package com.github.bluetiger9.nosql.benchmarking.clients;

public class ClientException extends Exception {
	private static final long serialVersionUID = 556403867530369147L;

	public ClientException() {
		super();
	}
	
	public ClientException(String message) {
		super(message);		
	}
	
	public ClientException(Throwable cause) {
		super(cause);
	}
	
	public ClientException(String message, Throwable cause) {
		super(message, cause);
	}
}
