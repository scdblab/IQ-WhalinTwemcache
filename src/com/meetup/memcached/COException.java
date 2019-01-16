package com.meetup.memcached;

public class COException extends Exception {
    private final String key;

	public COException(String message, String key) {
		super(message);
		this.key = key;
	}
	
	public String getKey() {
	    return key;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -6642921486512200999L;

}
