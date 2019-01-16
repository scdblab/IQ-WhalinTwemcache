package com.meetup.memcached.test;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.meetup.memcached.CASResponse;
import com.meetup.memcached.CASValue;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

public class MemcachedCASTest {

	static String key = "test";
	static String val = "lease";

	public static void main(String[] args) throws Exception {
		testHappycase();
		testKeyNotFound();
		testInvalidToken();
		testGetMulti();
		testGetsMulti();
	}
	
	private static void testGetMulti() throws Exception, IOException {
		MemcachedClient mc = init();
		mc.set(key, val);
		assert "lease".equals(mc.get(key));
		mc.delete("t");
		mc.delete("a");
		
		assert mc.get("t") == null;
		assert mc.get("a") == null;
		
		Map<String, Object> ret = mc.getMulti(new String[] {key, "t", "a"});
		assert ret.size() == 1;
		assert ret.get(key).equals(val);
		assert ret.get("t") == null;
		
		mc.cleanup();
	}
	
	private static void testGetsMulti() throws Exception, IOException {
		MemcachedClient mc = init();
		mc.set(key, val);
		assert "lease".equals(mc.get(key));
		mc.delete("t");
		mc.delete("a");
		
		assert mc.get("t") == null;
		assert mc.get("a") == null;
		
		Map<String, Object> ret = mc.getsMulti(new String[] {key, "t", "a"});
		assert ret.size() == 1;
		assert CASValue.class.isInstance(ret.get(key));
		
		CASValue cas = CASValue.class.cast(ret.get(key));
		CASResponse resp = mc.cas(key, "new_lease", cas.getCasToken());
		System.out.println(resp);
		assert CASResponse.SUCCESS.equals(resp);
		
		mc.cleanup();
	}
	
	private static void testInvalidToken() throws Exception, IOException {
		MemcachedClient mc = init();
		mc.set(key, val);
		assert "lease".equals(mc.get(key));

		CASValue cas = mc.gets(key);
		
		assert mc.set(key, val) == true;
		
		CASResponse resp = mc.cas(key, "new_lease", cas.getCasToken());
		System.out.println(resp);
		assert CASResponse.INVALID_CAS_TOKEN.equals(resp);
		mc.cleanup();
	}
	
	private static void testKeyNotFound() throws Exception, IOException {
		MemcachedClient mc = init();

		CASValue cas = mc.gets(key);
		assert cas == null;
		
		CASResponse resp = mc.cas(key, "new_lease", 0);
		System.out.println(resp);
		assert CASResponse.NOT_FOUND.equals(resp);
		mc.cleanup();
	}

	private static void testHappycase() throws Exception, IOException {
		MemcachedClient mc = init();
		mc.set(key, val);
		assert "lease".equals(mc.get(key));

		CASValue cas = mc.gets(key);
		CASResponse resp = mc.cas(key, "new_lease", cas.getCasToken());
		System.out.println(resp);
		assert CASResponse.SUCCESS.equals(resp);
		mc.cleanup();
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

		pool.setInitConn(1);
		pool.setMinConn(1);
		pool.setMaxConn(1);
		pool.setMaintSleep(20);

		pool.setNagle(false);
		pool.initialize();

		// get client instance
		MemcachedClient mc = new MemcachedClient("test");
		mc.setCompressEnable(false);

		mc.delete(key);
		return mc;
	}
}
