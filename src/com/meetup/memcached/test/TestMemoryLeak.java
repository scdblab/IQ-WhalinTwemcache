package com.meetup.memcached.test;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.meetup.memcached.CLValue;
import com.meetup.memcached.IQException;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

public class TestMemoryLeak {
	MemcachedClient mc;
	String[] serverlist = {	"localhost:11211" };
	
	public TestMemoryLeak() {
		ConsoleAppender console = new ConsoleAppender();
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.ALL);
		console.activateOptions();
		Logger.getRootLogger().addAppender(console);
	}
	
	public void startCache(String poolName) {
		Integer[] weights = {1}; 

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance( poolName );
		pool.setServers( serverlist );
		pool.setWeights( weights );
		pool.setMaxConn( 250 );
		pool.setNagle( false );
		pool.setHashingAlg( SockIOPool.CONSISTENT_HASH );
		pool.initialize();

		Logger.getRootLogger().setLevel(Level.INFO);

		mc = new MemcachedClient( "test" );

		ConsoleAppender console = new ConsoleAppender();
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.ALL);
		console.activateOptions();
		Logger.getRootLogger().addAppender(console);
		
		//testMemoryLeak3();
		//testMemoryLeak5();
		testEvict();
	}
	
	public void testEvict() {
		// assign cache to be 2 MB.
		String pref = "k";
		byte[] bytes = new byte[1020];	// value that is 1KB in size.
		new Random().nextBytes(bytes);
		
		for (int i = 0; i < 2048; i++) {
			String key = String.format("%s%d", pref, i);
			try {
				mc.set(key, bytes);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// check stats
		// make sure there is no item eviction or slab eviction.
		
		try {
			mc.set("testkey", bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// check stats, item eviction should happen; slab eviction should not happen.
		
		try {
			mc.set("testkey1", "xxxxx");	// put a small value to cache
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// check stats, slab eviction should happens.
		System.out.println("Test done.");
	}
	
	public static void main(String[] args) {
		TestMemoryLeak leak = new TestMemoryLeak();
//		leak.testMemoryLeak5();
	}
	
	public void testMemoryLeak5() {
		String key = "key";
		byte[] bytes = new byte[10];
		new Random().nextBytes(bytes);
		try {
			mc.set(key, bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			String sid = mc.generateSID();
			mc.ewread(sid, key, 0, false);
			mc.ewswap(sid, key, 0, null);
			mc.ewcommit(sid, 0, false);
		} catch (IQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void testMemoryLeak4() {
		String key = "key";
		int MAX = 1000000000;
		int loop = 30;
		
		byte[] bytes = new byte[10];
		new Random().nextBytes(bytes);
		
		for (int i = 0; i < MAX; i++) {
			try {
				mc.delete(key);
				mc.set(key, bytes);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for (int j = 0; j < loop; j++) {
				String sid = mc.generateSID();
				try {
					mc.ewappend(key, new Integer(0), bytes, sid);
					mc.swap(key, 0, bytes);
					mc.ewcommit(sid, 0, false);
				} catch (IQException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public void testMemoryLeak3() {
		String key = "key";
		int MAX = 1000000000;
		byte[] bytes = new byte[1000];
		new Random().nextBytes(bytes);
		
		try {
			mc.set(key, bytes);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for (int i = 0; i < MAX; i++) {
			try {
				String sid = mc.generateSID();
				mc.ewread(sid, key, 0, false);
				mc.ewswap(sid, key, bytes);
				mc.ewcommit(sid, 0, false);
			} catch (IQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void testMemoryLeak2() {
		String key = "key1";
		byte[] bytes = new byte[10];
		new Random().nextBytes(bytes);
		try {
			mc.set(key, bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int MAX = 1000000000;
		for (int i = 0; i < MAX; i++) {
			mc.get(key, 0, false);
		}
	}
	
	public void testMemoryLeak() {
		String prefix = "key";
		int count = 0;
		int MAX = 1000000000;
		byte[] bytes = new byte[10];
		new Random().nextBytes(bytes);
		
		for (int i = 0; i < MAX; i++) {
			String key = String.format("%s%d", prefix, count++);
			try {
				mc.cleanup();
				mc.ewget(key, 0);	
//				mc.delete(key);
				mc.iqset(key, bytes);	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			catch (IQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
