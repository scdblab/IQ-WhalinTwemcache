package com.meetup.memcached.test;

import java.util.Date;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.MemcachedLease;
import com.meetup.memcached.SockIOPool;

public class TestMemcachedLease {

	static String key = "test";

	public static void main(String[] args) throws Exception {
		
		Date date = new Date(1000);
		System.out.println(date.getTime());
		
//		testHappycase();
//		testMutualExclusive();
//		testMultipleLeases();
//		testExpire();
		testLocalLeaseOnly();
	}

	private static void testMultipleLeases() throws Exception, InterruptedException {
		MemcachedClient mc = init();
		MemcachedLease c1 = new MemcachedLease(1, mc, false);

		List<String> leases = c1.acquireLeasesOptimistic(5000, new String[] { key, "test2", "test3" });
		System.out.println(leases);
		assert leases.size() == 3;
		assert c1.acquireLease(key, 5000) == false;
		assert c1.acquireLease("test2", 5000) == false;
		assert c1.acquireLease("test3", 5000) == false;

		MemcachedLease c2 = new MemcachedLease(2, mc, false);
		assert c2.acquireLease(key, 5000) == false;
		assert c2.acquireLease("test2", 5000) == false;
		assert c2.acquireLease("test3", 5000) == false;

		Thread.sleep(1000);

		assert c1.releaseLease(new String[] { key, "test2", "test3" }).size() == 3;
	}

	private static void testMutualExclusive() throws Exception, InterruptedException {
		MemcachedClient mc = init();
		MemcachedLease c1 = new MemcachedLease(1, mc, false);

		assert c1.acquireLease(key, 5000) == true;
		assert c1.acquireLease(key, 5000) == false;

		MemcachedLease c2 = new MemcachedLease(2, mc, false);
		assert c2.acquireLease(key, 5000) == false;

		Thread.sleep(1000);

		assert c1.releaseLease(key) == true;
	}
	
	private static void testLocalLeaseOnly() throws Exception, InterruptedException {
		MemcachedClient mc = init();
		MemcachedLease lease = new MemcachedLease(1, null, true);

		assert lease.acquireLease(key, 1000) == true;

		assert lease.releaseLease(key) == true;
		
		Thread.sleep(1000);
		
		assert mc.get(key) == null;
		
		assert lease.acquireLease(key, 1000) == true;
		
		assert lease.releaseLease(key) == true;
	}

	private static void testExpire() throws Exception, InterruptedException {
		MemcachedClient mc = init();
		MemcachedLease lease = new MemcachedLease(1, mc, false);

		assert lease.acquireLease(key, 1000) == true;

		assert lease.releaseLease(key) == true;
		
		Thread.sleep(1000);
		
		assert mc.get(key) == null;
		
		assert lease.acquireLease(key, 1000) == true;
		
		assert lease.releaseLease(key) == true;
	}

	private static void testHappycase() throws Exception, InterruptedException {
		MemcachedClient mc = init();
		MemcachedLease lease = new MemcachedLease(1, mc, false);

		assert lease.acquireLease(key, 5000) == true;

		Thread.sleep(1000);

		assert lease.releaseLease(key) == true;
	}

	private static MemcachedClient init() throws Exception {

		ConsoleAppender console = new ConsoleAppender(); // create appender
		// configure the appender
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.ALL);
		console.activateOptions();
		// add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);

		String[] serverlist = { "127.0.0.1:11211" };

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance("test");
		pool.setServers(serverlist);

		pool.setInitConn(2);
		pool.setMinConn(2);
		pool.setMaxConn(10);
		pool.setMaintSleep(20);

		pool.setNagle(false);
		pool.initialize();

		// get client instance
		MemcachedClient mc = new MemcachedClient("test");
		mc.setCompressEnable(false);

		mc.delete(key);
		mc.delete("test2");
		mc.delete("test3");
		return mc;
	}

}
