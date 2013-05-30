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
