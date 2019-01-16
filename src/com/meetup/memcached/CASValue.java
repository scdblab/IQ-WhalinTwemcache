package com.meetup.memcached;

public class CASValue {
	private final Object value;
	private final long casToken;

	public CASValue(Object value, long casToken) {
		super();
		this.value = value;
		this.casToken = casToken;
	}

	public Object getValue() {
		return value;
	}

	public long getCasToken() {
		return casToken;
	}

	@Override
	public String toString() {
		return "CASValue [value=" + value + ", casToken=" + casToken + "]";
	}
	
}
