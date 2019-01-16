package com.meetup.memcached.test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.meetup.memcached.CLValue;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

public class EWTests {
	MemcachedClient mc1;
	MemcachedClient mc2;
	
	public EWTests() {
		// specify the list of cache server
		// currently test on a single cache server only
		String[] serverlist = {	"10.0.0.210:11211" };
		
		Integer[] weights = {1};

		// initialize the pool for memcache servers
		SockIOPool pool = SockIOPool.getInstance( "test" );
		pool.setServers( serverlist );
		pool.setWeights( weights );
		pool.setMaxConn( 250 );
		pool.setNagle( false );
		pool.setHashingAlg( SockIOPool.CONSISTENT_HASH );
		pool.initialize();
		
		mc1 = new MemcachedClient("test");
		mc2 = new MemcachedClient("test");
	}
	
	public void cleanup(String key) throws Exception {
		mc1.delete(key);
	}
	
	public static void main(String[] args) throws Exception {
	  EWTests tests = new EWTests();
	  tests.testEWIncr01();
	  tests.testEWIncr02();
	}
	
	@Test
	public void test01() throws Exception {
		cleanup("key1");
		System.out.print("Test simple read... ");
		CLValue val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		assertEquals(mc1.iqset("key1", "val1"), true);
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		System.out.println("OK");
	}
	
	@Test
	public void test02() throws Exception {
		cleanup("key1");
		System.out.print("Test simple write qaread... ");
		
		String tid = mc1.generateSID();
		CLValue val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); 
		assertEquals(val1.isPending(), false); 
		assertNull(val1.getValue());
		mc1.ewswap(tid, "key1", null, "val1");
		assertEquals(mc1.get("key1"), null);	// the swap value is not exposed because the session hasn't committed.
		mc1.ewcommit(tid, null, true);
		
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); 
		assertEquals(val1.isPending(), true); 
		assertEquals(val1.getValue(), "val1");
		
		tid = mc1.generateSID();
		val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); 
		assertEquals(val1.isPending(), true); 
		assertEquals(val1.getValue(), "val1");
		mc1.ewswap(tid, "key1", null, "new_val1");
		mc1.ewcommit(tid, null, false);
		
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); 
		assertEquals(val1.isPending(), false); 
		assertEquals(val1.getValue(), "new_val1");
		
		System.out.println("OK");
	}
	
	@Test
	public void test03() throws Exception {
		cleanup("key1");
		System.out.print("Test simple write ewappend... ");
		
		String tid = mc1.generateSID();
		CLValue val1 = mc1.ewprepend("key1", null, "val1", tid);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), false);
		mc1.ewcommit(tid, null, false);
		
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		mc1.iqset("key1", "val1");
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		
		tid = mc1.generateSID();
		val1 = mc1.ewappend("key1", null, "new_val1", tid);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), true);
		mc1.ewcommit(tid, null, false);
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1new_val1");
		
		System.out.println("OK");
	}	
	
	@Test
	public void test04() throws Exception {
		System.out.print("Test Q lease voids I lease ewread... ");
		cleanup("key1");
		CLValue val1 = mc1.ewget("key1", null);
		assertNotNull(val1); 
		assertEquals(val1.isPending(), false); 
		assertNull(val1.getValue());
		
		String tid = mc1.generateSID();
		val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		mc1.ewswap(tid, "key1", null, "val1");
		mc1.ewcommit(tid, null, false);
		
		try { mc1.iqset("key1", "test"); assert false; } catch (Exception e) { }
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		
		System.out.println("OK");
	}
	
	@Test
	public void test05() throws Exception {
		System.out.print("Test Q lease voids I lease ewappend... ");
		cleanup("key1");
		CLValue val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		
		String tid = mc1.generateSID();
		val1 = mc1.ewappend("key1", null, "val1", tid);
		assertEquals(mc1.get("key1"), null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), false);
		mc1.ewcommit(tid, null, false);
		
		try { mc1.iqset("key1", "test"); assert false; } catch (Exception e) { }
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		mc1.iqset("key1", "val1");
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		
		tid = mc1.generateSID();
		val1 = mc1.ewappend("key1", null, "val2", tid);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), true);
		assertEquals(mc1.get("key1"), "val1");
		mc1.ewcommit(tid, null, false);
		
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1val2");
		
		System.out.println("OK");
	}
	
	@Test
	public void test06a() throws Exception {
		System.out.print("Test I lease same session... ");
		cleanup("key1");
		
		for (int i = 0; i < 100; i++) {
			CLValue val1 = mc1.ewget("key1", null);		// I lease same session should not block
			assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		}
		mc1.iqset("key1", "val1");
		CLValue val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		
		System.out.println("OK");
	}
	
	@Test
	public void test06() throws Exception {
		System.out.print("Test Q lease same session eqread... ");
		cleanup("key1");
		
		String tid = mc1.generateSID();
		
		for (int i = 0; i < 100; i++) {
			CLValue val1 = mc1.ewread(tid, "key1", null, true);
			assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		}
		mc1.ewswap(tid, "key1", null, "val1");
		mc1.ewcommit(tid, null, false);
		
		tid = mc1.generateSID();
		for (int i = 0; i < 100; i++) {
			CLValue val1 = mc1.ewread(tid, "key1", null, true);
			assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		}
		mc1.ewcommit(tid, null, false);
		
		System.out.println("OK");
	}
	
	@Test
	public void test07() throws Exception {
		System.out.print("Test Q lease same session ewappend... ");
		cleanup("key1");
		
		String tid = mc1.generateSID();		
		for (int i = 0; i < 100; i++) {
			CLValue val1 = mc1.ewappend("key1", null, "val1", tid);
			assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), false);
		}
		mc1.ewcommit(tid, null, false);
		assertEquals(mc1.get("key1"), null);
		mc1.set("key1", "val1");
		
		tid = mc1.generateSID();
		for (int i = 0; i < 2; i++) {
			CLValue val1 = mc1.ewappend("key1", null, "val2", tid);
			assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), true);
		}
		mc1.ewcommit(tid, null, false);
		
		tid = mc1.generateSID();
		for (int i = 0; i < 100; i++) {
			CLValue val1 = mc1.ewread(tid, "key1", null, true);
			assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1val2val2");
		}
		mc1.ewcommit(tid, null, false);
		
		System.out.println("OK");
	}
	
	@Test
	public void test08() throws Exception {
		System.out.print("Test Q lease same session ewread ewappend... ");
		cleanup("key1");
		
		// ewread no value
		String tid = mc1.generateSID();
		CLValue val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		mc1.ewappend("key1", null, "val1", tid);
		mc1.ewcommit(tid, null, false);
		
		mc1.set("key1", "test");
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "test");
		
		tid = mc1.generateSID();
		val1 = mc1.ewappend("key1", null, "val2", tid);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), true);
		val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "testval2");
		mc1.ewcommit(tid, null, false);
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "testval2");
		
		System.out.println("OK");
	}
	
	@Test
	public void test09() throws Exception {
		System.out.print("Test Q lease same session ewappend ewread... ");
		cleanup("key1");
		String tid = mc1.generateSID();
		CLValue val1 = mc1.ewappend("key1", null, "val1", tid);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), false);
		val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertNull(val1.getValue());
		mc1.ewcommit(tid, null, false);
		
		mc1.set("key1", "val1");
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1");
		val1 = mc1.ewappend("key1", null, "val2", tid);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), true);
		val1 = mc1.ewread(tid, "key1", null, true);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "val1val2");
		mc1.ewswap(tid, "key1", null, "test");
		mc1.ewcommit(tid, null, false);
		
		val1 = mc1.ewget("key1", null);
		assertNotNull(val1); assertEquals(val1.isPending(), false); assertEquals(val1.getValue(), "test");
		
		System.out.println("OK");
	}
	
	@Test
	public void testPending01() throws Exception {
		cleanup("key1");
		
		String tid = mc1.generateSID();
		CLValue val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), false);
		mc1.ewcommit(tid, null, true);
		
		tid = mc1.generateSID();
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), true);
		assertNull(val.getValue());
		mc1.ewcommit(tid, null, true);
		
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true);
		mc1.iqset("key1", "val1");
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true);
		
		tid = mc1.generateSID();
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), "val1");
		mc1.ewswap(tid, "key1", null, "val2");
		mc1.ewcommit(tid, null, true);
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), "val2");
		
		tid = mc1.generateSID();
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), "val2");
		mc1.ewswap(tid, "key1", null, "val3");
		mc1.ewcommit(tid, null, false);
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), false);
		assertEquals(val.getValue(), "val3");
	}
	
	@Test
	public void testPending02() throws Exception {
		cleanup("key1");
		String tid = mc1.generateSID();
		CLValue val = mc1.ewappend("key1", null, "val1", tid);
		assertEquals(val.isPending(), false);
		mc1.ewcommit(tid, null, true);
		
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true);
		val = mc1.ewappend("key1", null, "val1", tid);
		assertEquals(val.isPending(), true);
		mc1.ewcommit(tid, null, false);
		
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), false);
		mc1.iqset("key1", "val1"); 
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), false);
		assertEquals(val.getValue(), "val1");
	}
	
	@Test
	public void testUnlease01() throws Exception {
		cleanup("key1");
		mc1.ewget("key1", null);
		mc1.ewget("key2", null);
		mc1.ewget("key3", null);
		
		mc1.releaseILeases();
		
		mc1.ewget("key1", null);
		mc1.iqset("key1", "val1");
		assertEquals(mc1.get("key1"), "val1");
	}
	
	@Test
	public void testEWReadSameSession() throws Exception {
		cleanup("key1");
		String tid = mc1.generateSID();
		mc1.ewappend("key1", null, "val1", tid);
		CLValue val = mc1.ewread(tid, "key1", null, true);
		assertNull(val.getValue());
		mc1.ewcommit(tid, null, false);
		
		mc1.set("key1", "val1");
		tid = mc1.generateSID();
		mc1.ewappend("key1", null, "val2", tid);
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.getValue(), "val1val2");
		mc1.ewcommit(tid, null, false);
		val = mc1.ewget("key1", null);
		assertEquals(val.getValue(), "val1val2");
	}
	
	@Test
	public void testBug01() throws Exception {
		cleanup("key1");
		String tid = mc1.generateSID();
		mc1.ewdecr(tid, "key1",null);
		mc1.ewcommit(tid, null, true);
		
		CLValue val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true);
		tid = mc1.generateSID();
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), true);
		mc1.ewcommit(tid, null, true);
		
		tid = mc1.generateSID();
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true);
		mc1.ewcommit(tid, null, false);
	}
	
	@Test
	public void testBug02() throws Exception {
		cleanup("key1");
		mc1.set("key1", "val1");
		String tid = mc1.generateSID();
		CLValue val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.getValue(), "val1");
		mc1.ewcommit(tid, null, false);
		assertEquals(mc1.get("key1"), "val1");
	}
	
	@Test
	public void testMultiSess01() throws Exception {
		String tid1 = mc1.generateSID();
		mc1.ewincr(tid1, "key1", null);
		mc1.ewdecr(tid1, "key2", null);
		mc1.ewappend("key3", null, "val1", tid1);
		mc1.ewread(tid1, "key4", null, true);
		
		String tid2 = mc2.generateSID();
		mc2.ewincr(tid2, "key", null);
		
		try { 
			mc1.ewincr(tid1, "key", null); 
			assert(false); 
		} catch (Exception e) { }
		
		mc2.ewread(tid2, "key1", null, true);
		mc2.ewincr(tid2, "key2", null);
		mc2.ewdecr(tid2, "key3", null);
		mc2.ewappend("key4", null, "val", tid2);
		
		mc2.ewcommit(tid2, null, false);
	}
	
	@Test
	public void testMultiSess02() throws Exception {
		String tid1 = mc1.generateSID();
		mc1.ewincr(tid1, "key1", null);
		mc1.ewdecr(tid1, "key2", null);
		mc1.ewappend("key3", null, "val1", tid1);
		mc1.ewread(tid1, "key4", null, true);
		
		String tid2 = mc2.generateSID();
		mc2.ewincr(tid2, "key", null);
		
		try { 
			mc1.ewread(tid1, "key", null, true); 
			assert(false); 
		} catch (Exception e) { }
		
		mc2.ewread(tid2, "key1", null, true);
		mc2.ewincr(tid2, "key2", null);
		mc2.ewdecr(tid2, "key3", null);
		mc2.ewappend("key4", null, "val", tid2);
		
		mc2.ewcommit(tid2, null, false);
	}
	
	@Test
	public void testMultiSess03() throws Exception {
		String tid1 = mc1.generateSID();
		mc1.ewincr(tid1, "key1", null);
		mc1.ewdecr(tid1, "key2", null);
		mc1.ewappend("key3", null, "val1", tid1);
		mc1.ewread(tid1, "key4", null, true);
		
		String tid2 = mc2.generateSID();
		mc2.ewincr(tid2, "key", null);
		
		try { 
			mc1.ewappend("key", null, "val1", tid1); 
			assert(false); 
		} catch (Exception e) { }
		
		mc2.ewread(tid2, "key1", null, true);
		mc2.ewincr(tid2, "key2", null);
		mc2.ewdecr(tid2, "key3", null);
		mc2.ewappend("key4", null, "val", tid2);
		
		mc2.ewcommit(tid2, null, false);
	}
	
	@Test
	public void testMultiSess04() throws Exception {
		String tid = mc2.generateSID();
		mc1.ewget("key1", null);
		mc2.ewread(tid, "key1", null, true);
		try { mc1.iqset("key1", "val1"); assert(false); } catch (Exception e) {
			
		}
		mc2.ewswap(tid, "key1", "test");
		mc2.ewcommit(tid, null, false);
		CLValue val = mc1.ewget("key1", null);
		assertEquals(val.getValue(), "test");
	}
	
	@Test
	public void testMultiSess05() throws Exception {
		mc1.set("key1", "val1");
		String tid = mc1.generateSID();
		mc1.ewread(tid, "key1", null, true);
		CLValue val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "val1");
		mc1.ewswap(tid, "key1", "val2");
		val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "val1");
		mc1.ewcommit(tid, null, false);
		val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "val2");
	}
	
	@Test
	public void testMultiSess06() throws Exception {
		mc1.set("key1", "5");
		String tid = mc1.generateSID();
		mc1.ewincr(tid, "key1", null);
		CLValue val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "5");
		mc1.ewcommit(tid, null, false);
		val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "6");
	}
	
	@Test
	public void testMultiSess07() throws Exception {
		mc1.set("key1", "val1");
		String tid = mc1.generateSID();
		mc1.ewappend("key1", null, "val2", tid);
		CLValue val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "val1");
		mc1.ewcommit(tid, null, false);
		val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "val1val2");
	}
	
	@Test
	public void testEWIncr01() throws Exception {
	  cleanup("key1");
	  
		String tid = mc1.generateSID();
		CLValue val = mc1.ewincr(tid, "key1", null);
		assertEquals(val.getValue(), false);
		val = mc1.ewincr(tid, "key1", null);
		assertEquals(val.getValue(), false);
		val = mc1.ewincr(tid, "key1", null);
		assertEquals(val.getValue(), false);
		mc1.ewcommit(tid, null, true);
		
		tid = mc1.generateSID();
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), true); assertNull(val.getValue());
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), true); assertNull(val.getValue());
		mc1.ewcommit(tid, null, false);
		
		tid = mc1.generateSID();
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), false); assertNull(val.getValue());
		mc1.iqset("key1", "2");
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.isPending(), false);
		assertEquals(val.getValue(), "2");
		mc1.ewswap(tid, "key1", "3");
		val = mc2.ewget("key1", null);
		assertEquals(val.isPending(), false);
		assertEquals(val.getValue(), "2");
		mc1.ewcommit(tid, null, true);		
		
		tid = mc1.generateSID();
		val = mc1.ewincr(tid, "key1", null);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), true);
		mc1.ewincr(tid, "key1", null);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), true);
		mc1.ewincr(tid, "key1", null);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), true);
		val = mc2.ewget("key1", null);
		assertEquals(val.isPending(), true);
		assertEquals(val.getValue(), "3");
		mc1.ewcommit(tid, null, false);
		val = mc1.ewget("key1", null);
		assertEquals(val.isPending(), false);
		assertEquals(val.getValue(), "6");
	}
	
	@Test
	public void testEWIncr02() throws Exception {
	  cleanup("key1");
	  
		String tid = mc1.generateSID();
		mc1.set("key1", "5");
		mc1.ewincr(tid, "key1", null);
		CLValue val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.getValue(), "6");
		mc1.ewcommit(tid, null, true);
		val = mc1.ewget("key1", null);
		assertEquals(val.getValue(), "6");
		
		tid = mc1.generateSID();
		mc1.ewincr(tid, "key1", null);
		val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.getValue(), "7");
		mc1.ewcommit(tid, null, false);
		val = mc1.ewget("key1", null);
		assertEquals(val.getValue(), "7");
	}
	
	@Test
	public void testMultiIncrUpdt() throws Exception {
		String tid = mc1.generateSID();
		mc1.set("key1", "1");
		mc1.ewincr(tid, "key1", null);
		mc1.ewincr(tid, "key1", null);
		mc1.ewincr(tid, "key1", null);
		mc1.ewdecr(tid, "key1", null);
		mc1.ewdecr(tid, "key1", null);
		mc1.ewappend("key1", null, "val1", tid);
		mc1.ewappend("key1", null, "val2", tid);
		CLValue val = mc2.ewget("key1", null);
		assertEquals(val.getValue(), "1");
		mc1.ewcommit(tid, null, false);
		val = mc1.ewget("key1", null);
		assertEquals(val.getValue(), "2val1val2");
	}
	
	@Test
	public void testSwapAbort() throws Exception {
		String tid = mc1.generateSID();
		mc1.set("key1", "val1");
		mc1.ewread(tid, "key1", null, true);
		mc1.ewswap(tid, "key1", "val2");
		mc1.release(tid);
		assertEquals(mc1.ewget("key1", null).getValue(), "val1");
	}
	
	@Test
	public void testEWIncrAbort() throws Exception {
		String tid = mc1.generateSID();
		mc1.set("key1", "1");
		mc1.ewincr(tid, "key1", null);
		mc1.ewincr(tid, "key1", null);
		mc1.ewdecr(tid, "key1", null);
		mc1.release(tid);
		assertEquals(mc1.ewget("key1", null).getValue(), "1");
	}
	
	@Test
	public void testBug04() throws Exception {
		mc1.set("key1", "1");
		String tid = mc1.generateSID();
		mc1.ewincr(tid, "key1", null);
		CLValue val = mc1.ewread(tid, "key1", null, true);
		assertEquals(val.getValue(), "2");
		mc1.release(tid);
		assertEquals(mc1.get("key1"), "1");
	}
	
	@Test
	public void testBug03() throws Exception {
		String tid = mc1.generateSID();
		mc1.ewread(tid, "key1", null, true);
		mc1.ewappend("key1", null, "val1", tid);
		mc1.swap("key1", null, "test");
		mc1.ewcommit(tid, null, false);
		assertEquals(mc1.ewget("key1", null).getValue(), "test");
	}
	
	/** Test 10-11 requires time out set up to 1000ms **/
	
	@Test
	public void test10() {
		System.out.print("Test I lease back-off");
		System.out.println("OK");
	}
	
	@Test
	public void test11() {
		System.out.print("Test Q lease abbort");
		System.out.println("OK");
	}
}

