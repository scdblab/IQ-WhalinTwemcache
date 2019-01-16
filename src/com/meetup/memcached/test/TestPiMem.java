package com.meetup.memcached.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.meetup.memcached.CLValue;
import com.meetup.memcached.MemcachedClient;

public class TestPiMem extends TestMemoryLeak {
	public static void main(String[] args) {
		TestPiMem test = new TestPiMem();
		try {
//			test.testPiMem01();
//			test.testPiMem02();
//			test.testPiMem03();
			test.testPiMem05();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testPiMem04() throws Exception {
		startCache("test");
		MemcachedClient mc = new MemcachedClient("test");
		
		Set<String> set = new HashSet<String>();
		String key = "key";
		int keyRange = 10;
		int valRange = 1000;
		Random rand = new Random();
		int addKey = 11;
		for (int i = 0; i < 1000; i++) {
			String k =  String.format("%s%d", key, rand.nextInt(keyRange));
			byte[] b = new byte[rand.nextInt(valRange)+1];
			
			boolean success = mc.append(k, b);
			if (!success) {
				Object obj = mc.get(k);
				if (obj != null) {
					success = mc.set(String.format("%s%d",  key, addKey++), obj);
					if (!success) {
						continue;
					}
				}
			}
		}
	}
	
	public void testPiMem01() throws Exception {
		startCache("test");
		MemcachedClient mc = new MemcachedClient("test");
		
		String key = "key1";
		byte[] bytes = new byte[100];
		for (int i = 0; i < 1000; i++)
			mc.set(key, bytes, null, true);
		
		mc.delete(key);
		
		System.out.println("SUCCESS...");
	}
	
	public void testPiMem02() throws Exception {
		startCache("test");
		MemcachedClient mc = new MemcachedClient("test");
		String key = "key1";
		
		byte[] bytes = new byte[1000];
		mc.set(key, bytes, null, true);
		for (int i = 0; i < 2000; i++)
			mc.append(key, null, bytes, true);
		mc.delete(key);
		System.out.println("SUCCESS...");
	}
	
	public void testPiMem03() throws Exception {
		startCache("test");
		MemcachedClient mc = new MemcachedClient("test");
		String key = "key1";
		
		byte[] bytes = new byte[1000];
		
		for (int i = 0; i < 1000; i++) {
			String tid = mc.generateSID();
			mc.ewread(tid, key, 0, false);
			mc.swap(key, 0, bytes, true);
			mc.ewcommit(tid, 0, false);
		}
		
		for (int i = 0; i < 1000; i++) {
			String tid = mc.generateSID();
			mc.ewread(tid, key, 0, false);
			mc.swap(key, 0, bytes, true);
			mc.ewcommit(tid, 0, false);
			
			tid = mc.generateSID();
			mc.ewread(tid, key, 0, false);
			mc.swap(key, 0, null, true);
			mc.ewcommit(tid, 0, false);
		}
		
		mc.delete(key);
		System.out.println("SUCCESS...");
	}
	
	public void testPiMem05() throws IOException {
		startCache("test");
		MemcachedClient mc = new MemcachedClient("test");
		
		String key = "key";
		for (int i = 0; i < 10; i++) {
			try {
				CLValue value = mc.ewget(key, 0);
				if (value.getValue() != null) {
					System.out.println("Value should be null");
					return;
				}
				
				mc.releaseILeases(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
