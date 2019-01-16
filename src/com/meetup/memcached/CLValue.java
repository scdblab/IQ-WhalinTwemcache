package com.meetup.memcached;

/**
 * Contextual-Lease Value
 * @author hieun
 *
 */
public class CLValue {
	private boolean pending;
	private Object value;
	
	public CLValue(Object value, boolean pending) {
		this.value = value;
		this.pending = pending;
	}
	
	public boolean isPending() {
		return pending;
	}
	
	public Object getValue() {
		return value;
	}
}
