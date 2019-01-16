package com.meetup.memcached;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class MemcachedLease {

	private static final long JITTER_BASE = 30;
	private static final int JITTER_RANGE = 20;
	private static final String NEW_LEASE = "n";

	private final int clientId;
	private final MemcachedClient memcachedClient;
	private final Random random;
	private boolean onlyLocalLease = false;
	
  public final static AtomicLong numLeasesGranted = new AtomicLong(0);
	private static final AtomicInteger backoff = new AtomicInteger(0);
	
	public static AtomicInteger getBackoff() {
		return backoff;
	}

	/**
	 * this map is used locally to prevent unnecessary traffic to memcached if a
	 * lease is acquired locally. This is a simple version which doesn't check
	 * lease has expired locally or not. It doesn't prevent the case where
	 * multiple threads blocked on a key acquired by another local thread which
	 * has died before releasing its lease. A robust version can be developed if
	 * this becomes an issue.
	 */
	private static final ConcurrentHashMap<String, String> localKeyLeaseMap = new ConcurrentHashMap<>();
	/**
	 * this counter is incremented on every invocation on acquire lease method
	 * to enable concurrency locally.
	 */
	private final AtomicInteger counter = new AtomicInteger();

	private final Logger logger = Logger.getLogger(MemcachedLease.class);

	public MemcachedLease(int clientId, MemcachedClient memcachedClient, boolean onlyLocalLease) {
		super();
		this.clientId = clientId;
		this.memcachedClient = memcachedClient;
		this.random = new Random();
		this.onlyLocalLease = onlyLocalLease;
	}

//	public void acquireLeases(long timeout, String... keys) {
//		while (true) {
//			int i = 0;
//			for (; i < keys.length; i++) {
//				if (!acquireLease(keys[i], timeout)) {
//					break;
//				}
//			}
//			if (i == keys.length) {
//				break;
//			} else {
//				for (int j = 0; j < i; j++) {
//					releaseLease(keys[j]);
//				}
//				try {
//					Thread.sleep(JITTER_BASE + random.nextInt(JITTER_RANGE));
//				} catch (Exception e) {
//					logger.error("acquire leases failed on keys " + keys, e);
//				}
//			}
//		}
//	}

	public void acquireTillSuccess(String key, Integer hashCode, long timeout) {
		logger.debug("acquire lease on key " + key);
		int tries = 0;
		while (!acquireLease(key, hashCode, timeout)) {
			try {
				Thread.sleep(JITTER_BASE + random.nextInt(JITTER_RANGE));
			} catch (Exception e) {
				logger.error("acquire leases failed on key " + key, e);
			}
//			if (tries >= 50) {
//				logger.error("acquire lease on key failed for > 50 times " + key);
//			}
			tries++;
			backoff.incrementAndGet();
		}
		
		numLeasesGranted.incrementAndGet();
	}

	/**
	 * @param timeout
	 * @param keys
	 * @return list of keys that successfully acquired
	 */
	public List<String> acquireLeasesOptimistic(long timeout, String... keys) {

		String newVal = this.clientId + " " + this.counter.incrementAndGet();
		List<String> acquiredLocal = new ArrayList<>();
		for (String key : keys) {
			if (this.localKeyLeaseMap.putIfAbsent(key, newVal) == null) {
				acquiredLocal.add(key);
			}
		}

		Map<String, Object> getsVal = this.memcachedClient.getsMulti(acquiredLocal.toArray(new String[0]));
		List<String> acquiredLeases = new ArrayList<>();
		for (String key : acquiredLocal) {
			CASValue cas = CASValue.class.cast(getsVal.get(key));
			if (acquireLease(key, timeout, cas, newVal)) {
				acquiredLeases.add(key);
			} else {
				// failed to acquire lease in memcached, remove it locally
				this.localKeyLeaseMap.remove(key);
			}
		}
		return acquiredLeases;
	}
	
	public boolean acquireLease(String key, long timeout) {
	  return acquireLease(key, null, timeout);
	}

	public boolean acquireLease(String key, Integer hashCode, long timeout) {

		String newVal = this.clientId + " " + this.counter.incrementAndGet();
		if (localKeyLeaseMap.putIfAbsent(key, newVal) == null) {
			
			if (onlyLocalLease) {
				return true;
			}
			
			boolean success = false;
			CASValue val = this.memcachedClient.gets(key, hashCode, false);
			success = acquireLease(key, hashCode, timeout, val, newVal);

			if (!success) {
				localKeyLeaseMap.remove(key);
			}

			return success;
		}

		return false;
	}
	
	private boolean acquireLease(String key, long timeout, CASValue val, String newVal) {
	  return acquireLease(key, null, timeout, val, newVal);
	}
	
	private boolean acquireLease(String key, Integer hashCode, long timeout, CASValue val, String newVal) {
		try {
			if (val == null) {
				return this.memcachedClient.add(key, newVal, new Date(timeout), hashCode);
			} else if (NEW_LEASE.equals(val.getValue())) {
				return CASResponse.SUCCESS.equals(this.memcachedClient.cas(key, newVal, new Date(timeout), val.getCasToken(), hashCode));
			}
		} catch (Exception e) {
			logger.error("acquire leases failed on key " + key, e);
		}
		return false;
	}
	
	public boolean releaseLease(String key) {
	  return releaseLease(key, null);
	}

	public boolean releaseLease(String key, Integer hashCode) {
		
		logger.debug("release lease on key " + key);
		if (onlyLocalLease) {
			this.localKeyLeaseMap.remove(key);
			return true;
		}
		
		CASValue val = this.memcachedClient.gets(key, hashCode, false);
		boolean ret = releaseLease(key, hashCode, val);
		this.localKeyLeaseMap.remove(key);
		return ret;
	}

	private boolean releaseLease(String key, Integer hashCode, CASValue val) {
		if (val == null || !val.getValue().equals(this.localKeyLeaseMap.get(key))) {
			System.out.println("BUG: release lease failed on key " + key + " val: " + val);
			logger.fatal("BUG: release lease failed on key " + key + " val: " + val);
			return false;
		}
		CASResponse response;
		try {
			response = this.memcachedClient.cas(key, NEW_LEASE, new Date(1000), val.getCasToken(), hashCode);
			if (CASResponse.SUCCESS.equals(response)) {
				return true;
			} else {
				this.memcachedClient.delete(key, hashCode, null);
			}
			System.out.println("BUG: release lease failed on key " + key + " val: " + val);
			logger.fatal("BUG: release lease failed on key " + key + " response: " + response);
		} catch (Exception e) {
			logger.fatal("BUG: release lease failed on key " + key, e);
		}

		return false;
	}

	/**
	 * @param key
	 * @return list of keys that successfully
	 */
	public List<String> releaseLease(String... keys) {
		Map<String, Object> getsVal = this.memcachedClient.getsMulti(keys);
		List<String> releasedLeases = new ArrayList<>();
		getsVal.forEach((key, val) -> {
			CASValue cas = CASValue.class.cast(val);
			if (releaseLease(key, null, cas)) {
				releasedLeases.add(key);
			}
			this.localKeyLeaseMap.remove(key);
		});
		return releasedLeases;
	}
	
	public static void main(String[] args) {
		System.out.println(0-2147483647);
	}

}
